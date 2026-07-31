import polars as pl
from sqlalchemy import text
from Credentials import engine_DEV_Test as engine
from pathlib import Path
import pandas as pd
import duckdb
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import traceback
import logging
from zoneinfo import ZoneInfo
import os

onedrive = next(p for p in Path(os.environ["USERPROFILE"]).iterdir()
                if p.name.startswith("OneDrive - "))

# ----------------- LOGGING SETUP -----------------
central = ZoneInfo("America/Chicago")
now_central = datetime.now(central)
current = now_central.strftime("%Y-%m-%d")
current_year = now_central.year
log_file = onedrive / "LogFile" / "BCDA" / f"{current_year}_BCDA_V3_DownloadLog.log"


log_file.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logging.getLogger().addHandler(console_handler)

table_names = [
    "dbo.eob_adjudication_staging_v3",
    "dbo.eob_base_staging_v3",
    "dbo.eob_diagnosis_staging_v3",
    "dbo.eob_total_staging_v3",
    "dbo.eob_procedure_staging_v3",
    "dbo.eob_item_staging_v3",
    "dbo.eob_item_adjudication_staging_v3",
    "dbo.eob_item_ext_staging_v3",
    "dbo.eob_supporting_staging_v3",
    "dbo.eob_careteam_staging_v3",
    'eob_hemoglobin_staging_v3',
]

file_path = Path(r'C:\BCDA_V3\Data')

now = datetime.now().strftime('%m-%d-%Y %H:%M:%S')

MIN_SQL_DATE = datetime(1753, 1, 1)

def truncate_tables(table_names: list):
    for table in table_names:
        with engine.begin() as con:
            con.execute(text(f"""
            IF OBJECT_ID('{table}', 'U') IS NOT NULL 
            TRUNCATE TABLE {table};
            """))
            
def safe_expr(df, source_col, expr, alias):
    if source_col in df.columns:
        return expr.alias(alias)
    return pl.lit(None).alias(alias)

def flatten(df: pl.DataFrame, col: str) -> pl.DataFrame:
    return df.explode(col).unnest(col)

def load_to_sql(df: pd.DataFrame, table_name: str):
    if df.empty:
        return
    with engine.begin() as conn:
        df.to_sql(table_name, conn, if_exists='append', index=False, chunksize=5000)

def process_eob_base(df_eob: pl.DataFrame):  
    
    column_names = [
            'claim_id',
            'cntrl_num',
            'billablePeriod_start',
            'billablePeriod_end',
            'payment_amount',
            'payment_date',
            'patient_id',
            'provider_id',
            'status',
            'outcome',
            'type_code',
            'type_display',
            'type_code_display',
            'related_value',
            'related_relationship',
            'source',
            'final_action',
            'created',
            'lastUpdated',
            'filename',
            'extract_date'
    ]
    
    df_eob_base = (
        df_eob
        .explode('identifier')
        .with_columns(
            pl.col('identifier').struct.field('value').alias('cntrl_num'),
            pl.col('identifier').struct.field('system').alias('idt_system'),
            pl.col('id').alias('claim_id')
        )
        .filter(pl.col('idt_system').is_not_null())
        .with_columns(
            pl.lit(now).alias('extract_date'),
            pl.col("filename").str.split("/").list.get(-1).alias("filename")
        )
    )
    
    expres = [
        safe_expr(
            df_eob_base,
            'billablePeriod',
            pl.col('billablePeriod').struct.field('start'),
            'billablePeriod_start'
        ),
        safe_expr(
            df_eob_base,
            'billablePeriod',
            pl.col('billablePeriod').struct.field('end'),
            'billablePeriod_end'
        ),
        safe_expr(
            df_eob_base,
            'payment',
            pl.col('payment').struct.field('date'),
            'payment_date'
        ),
        safe_expr(
            df_eob_base,
            'patient',
            pl.col('patient').struct.field('reference').str.split('/').list.get(1),
            'patient_id'
        ),
        safe_expr(
            df_eob_base,
            'provider',
            pl.col('provider').struct.field('reference').str.replace('#',''),
            'provider_id'
        ),
        safe_expr(
            df_eob_base,
            'cntrl_num',
            pl.col('cntrl_num').str.replace_all(r'\s+',', '),
            'cntrl_num'
        ),
        safe_expr(
            df_eob_base,
            'type',
            pl.col('type').struct.field('coding').list.get(0).struct.field('code'),
            'type_code'
        ),
        safe_expr(
            df_eob_base,
            'type',
            pl.col('type').struct.field('coding').list.get(0).struct.field('display').str.to_titlecase(),
            'type_display'
        ),
        safe_expr(
            df_eob_base,
            'type',
            pl.col('type').struct.field('coding').list.get(1).struct.field('code').str.to_titlecase(),
            'type_code_display'
        ),
        safe_expr(
            df_eob_base,
            'meta',
            pl.col('meta').struct.field('lastUpdated'),
            'lastUpdated'
        ),
        safe_expr(
            df_eob_base,
            'meta',
            pl.col('meta').struct.field('tag').list.get(0).struct.field('code'),
            'source'
        ),
        safe_expr(
            df_eob_base,
            'meta',
            pl.col('meta').struct.field('tag').list.get(1).struct.field('code'),
            'final_action'
        ),
        safe_expr(
            df_eob_base,
            'related',
            pl.col('related').list.get(0).struct.field('reference').struct.field('value'),
            'related_value'
        ),
        safe_expr(
            df_eob_base,
            'related',
            pl.col('related').list.get(0).struct.field('relationship').struct.field('coding').list.get(0).struct.field('code'),
            'related_relationship'
        ),
    ]
    
    if 'payment' in df_eob_base.columns:
        if 'amount' in df_eob_base.unnest('payment').columns:
            pay_amount_expres= safe_expr(
                df_eob_base,
                'payment',
                pl.col('payment').struct.field('amount').struct.field('value'),
                'payment_amount'
            )
        else:
            pay_amount_expres= safe_expr(
                df_eob_base,
                'payment',
                pl.lit(None),
                'payment_amount'
            )
    else:
        pay_amount_expres= safe_expr(
            df_eob_base,
            'payment',
            pl.col('payment').struct.field('amount').struct.field('value'),
            'payment_amount'
        )
    
    df_eob_base = (
        df_eob_base
        .with_columns(expres)
        .with_columns(pay_amount_expres)
        .select(column_names)
    )
    
    return df_eob_base.to_pandas()

def process_eob_hemoglobin(df_eob: pl.DataFrame):
    columns_names = [
        'claim_id',
        'id',
        'test_code',
        'test_display',
        'test_value',
        'test_unit',
        'performer_id',
        'performer_id_type',
        'lastUpdated',
        'filename',
        'extract_date'
    ]
    if 'code' in df_eob.select('contained').explode('contained').unnest('contained').columns:
        

        
        df_hemoglobin = (
            df_eob
            .with_columns(
                pl.col('id').alias('claim_id'),
                pl.col('meta').struct.field('lastUpdated').alias('lastUpdated'),
                pl.col('filename').str.split('/').list.get(-1).alias('filename'),
                pl.lit(now).alias('extract_date')
            )
            .select(
                'claim_id',
                'contained',
                'lastUpdated',
                'filename',
                'extract_date'
            )
            .explode('contained')
            .unnest('contained')
            .filter(pl.col('code').is_not_null())
        )
        
        expres = [
            safe_expr(
                df_hemoglobin,
                'code',
                pl.col('code').struct.field('coding').list.get(0).struct.field('code'),
                'test_code'
            ),
            safe_expr(
                df_hemoglobin,
                'code',
                pl.col('code').struct.field('coding').list.get(0).struct.field('display'),
                'test_display'
            ),
            safe_expr(
                df_hemoglobin,
                'valueQuantity',
                pl.col('valueQuantity').struct.field('value'),
                'test_value'
            ),
            safe_expr(
                df_hemoglobin,
                'valueQuantity',
                pl.col('valueQuantity').struct.field('unit'),
                'test_unit'
            ),
            safe_expr(
                df_hemoglobin,
                'performer',
                pl.col('performer').list.get(0).struct.field('identifier').struct.field('value'),
                'performer_id'
            ),
            safe_expr(
                df_hemoglobin,
                'performer',
                pl.col('performer').list.get(0).struct.field('identifier').struct.field('system').str.split('/').list.get(-1),
                'performer_id_type'
            )
        ]
        df_hemoglobin = (
                df_hemoglobin
                .with_columns(expres)
                .select(columns_names)
        )
    else:
        df_hemoglobin = pl.DataFrame(schema = columns_names)
    return df_hemoglobin.to_pandas()

def process_eob_adjudication(df_eob: pl.DataFrame):
    
    column_names = [
        'claim_id',
        'category_display',
        'reason_display',
        'amount',
        'value',
        'lastUpdated',
        'filename',
        'extract_date'
    ]

    df_eob_adjudication = (
        df_eob
        .filter(pl.col('adjudication').is_not_null())
        .select(
            'id',
            'adjudication',
            'meta',
            'filename'
        )
        .pipe(flatten, 'adjudication')
        .with_columns(
            pl.col('id').alias('claim_id'),
            pl.col('meta').struct.field('lastUpdated'),
            pl.col('filename').str.split('/').list.get(-1).alias('filename'),
            pl.lit(now).alias('extract_date'),
        )
    )
    expres = [
        safe_expr(
            df_eob_adjudication,
            'category',
            pl.col('category').struct.field('coding').list.get(0).struct.field('display'),
            'category_display'
        ),
        safe_expr(
            df_eob_adjudication,
            'reason',
            pl.col('reason').struct.field('coding').list.get(0).struct.field('display'),
            'reason_display'
        ),
        safe_expr(
            df_eob_adjudication,
            'amount',
            pl.col('amount').struct.field('value'),
            'amount'
        ),
        safe_expr(
            df_eob_adjudication,
            'value',
            pl.col('value'),
            'value'
        )
    ]
    df_eob_adjudication = (
        df_eob_adjudication
        .with_columns(expres)
        .select(column_names)
    )
    return df_eob_adjudication.to_pandas()

def process_eob_diagnosis(df_eob: pl.DataFrame):
    
    column_names = (
            'claim_id',
            'sequence',
            'diagnosis_code',
            'diagnosis_system',
            'diagnosis_type_code',
            'onAdmission',
            'patient_id',
            'lastUpdated',
            'filename',
            'extract_date'
        )
    
    if 'diagnosis' not in df_eob.columns:
        return pd.DataFrame(columns= column_names)
    
    df_eob_diagnosis = (
        df_eob
        .filter(pl.col('diagnosis').is_not_null())
        .select(
            'id',
            'diagnosis',
            'meta',
            'patient',
            'filename'
        )
        .pipe(flatten, 'diagnosis')
        .with_columns(
            pl.lit(now).alias('extract_date'),
            pl.col('id').alias('claim_id'),
            pl.col("filename").str.split("/").list.get(-1).alias("filename")
        )
    )
    
    expres = [
        safe_expr(
            df_eob_diagnosis,
            'diagnosisCodeableConcept',
            pl.col('diagnosisCodeableConcept').struct.field('coding').list.get(0).struct.field('code'),
            'diagnosis_code'
        ),
        safe_expr(
            df_eob_diagnosis,
            'diagnosisCodeableConcept',
            pl.col('diagnosisCodeableConcept').struct.field('coding').list.get(0).struct.field('system').str.split('/').list.get(-1),
            'diagnosis_system'
        ),
        safe_expr(
            df_eob_diagnosis,
            'type',
            pl.col('type').list.get(0).struct.field('coding').list.get(0).struct.field('code').str.to_titlecase(),
            'diagnosis_type_code'
        ),
        safe_expr(
            df_eob_diagnosis,
            'meta',
            pl.col('meta').struct.field('lastUpdated'),
            'lastUpdated'
        ),
        safe_expr(
            df_eob_diagnosis,
            'patient',
            pl.col('patient').struct.field('reference').str.split('/').list.get(1),
            'patient_id'
        ),
        safe_expr(
            df_eob_diagnosis,
            'onAdmission',
            pl.col('onAdmission').struct.field('coding').list.get(0).struct.field('code'),
            'onAdmission'
        ),
    ]
    
    df_eob_diagnosis = (
        df_eob_diagnosis
        .with_columns(expres)
        .select(column_names)
    )
    return df_eob_diagnosis.to_pandas()

def process_eob_procedure(df_eob: pl.DataFrame):
    
    column_names = [
            'claim_id',
            'sequence',
            'procedure_code',
            'code_system',
            'type_code',
            'patient_id',
            'lastUpdated',
            'filename',
            'extract_date'
    ]
    
    if 'procedure' not in df_eob.columns:
        return pd.DataFrame(columns = column_names)
    
    df_eob_procedure = (
        df_eob
        .filter(pl.col('procedure').is_not_null())
        .select(
            'id',
            'procedure',
            'meta',
            'patient',
            'filename'
        )
        .pipe(flatten, 'procedure')
        .with_columns(
            pl.lit(now).alias('extract_date'),
            pl.col('id').alias('claim_id'),
            pl.col("filename").str.split("/").list.get(-1).alias("filename")
        )
    )
    
    expres = [
        safe_expr(
            df_eob_procedure,
            'procedureCodeableConcept',
            pl.col('procedureCodeableConcept').struct.field('coding').list.get(0).struct.field('code'),
            'procedure_code'
        ),
        safe_expr(
            df_eob_procedure,
            'procedureCodeableConcept',
            pl.col('procedureCodeableConcept').struct.field('coding').list.get(0).struct.field('system').str.split('/').list.get(-1),
            'code_system'
        ),
        safe_expr(
            df_eob_procedure,
            'type',
            pl.col('type').list.get(0).struct.field('coding').list.get(0).struct.field('code'),
            'type_code'
        ),
        safe_expr(
            df_eob_procedure,
            'meta',
            pl.col('meta').struct.field('lastUpdated'),
            'lastUpdated'
        ),
        safe_expr(
            df_eob_procedure,
            'patient',
            pl.col('patient').struct.field('reference').str.split('/').list.get(1),
            'patient_id'
        )
    ]
    
    df_eob_procedure = (
        df_eob_procedure
        .with_columns(expres)
        .select(column_names)
    )
    
    return df_eob_procedure.to_pandas()



def process_eob_total(df_eob: pl.DataFrame):
    
    column_names = [
            'claim_id',
            'total_amount',
            'category_code',
            'patient_id',
            'lastUpdated',
            'filename',
            'extract_date'
    ]
    
    df_eob_total = (
        df_eob
        .filter(pl.col('total').is_not_null())
        .select(
            'id',
            'total',
            'patient',
            'meta',
            'filename'
        )
        .pipe(flatten, 'total')
        .with_columns(
            pl.lit(now).alias('extract_date'),
            pl.col('id').alias('claim_id'),
            pl.col("filename").str.split("/").list.get(-1).alias("filename")
        )
    )
    
    expres = [
        safe_expr(
            df_eob_total,
            'amount',
            pl.col('amount').struct.field('value'),
            'total_amount'
        ),
        safe_expr(
            df_eob_total,
            'patient',
            pl.col('patient').struct.field('reference').str.split('/').list.get(1),
            'patient_id'
        ),
        safe_expr(
            df_eob_total,
            'meta',
            pl.col('meta').struct.field('lastUpdated'),
            'lastUpdated'
        ),
        safe_expr(
            df_eob_total,
            'category',
            pl.col('category').struct.field('coding').list.get(1).struct.field('display'),
            'category_code'
        ),
    ]
    
    df_eob_total = (
        df_eob_total
        .with_columns(expres)
        .select(column_names)
    )
    
    return df_eob_total.to_pandas()

def build_item_df(df_eob: pl.DataFrame):
    return (
        df_eob
        .filter(pl.col("item").is_not_null())
        .select(
            "id",
            "item",
            "patient",
            "meta",
            "filename",
        )
        .pipe(flatten, "item")
        .with_columns(
            pl.lit(now).alias("extract_date"),
            pl.col('id').alias('claim_id'),
            pl.col("filename").str.split("/").list.get(-1).alias("filename")
        )
    )

def process_eob_item(df_eob: pl.DataFrame):
    
    column_names = [
            'claim_id',
            'sequence',
            'diagnosisSequence',
            'informationSequence',
            'location_code',
            'location_display',
            'product_service_code',
            'modifier',
            'product_service_system',
            'quantity_value',
            'revenue_code',
            'revenue_display',
            'service_start',
            'service_end',
            'servicedDate',
            'patient_id',
            'lastUpdated',
            'filename',
            'extract_date'
    ]
    
    expr = [
        safe_expr(
            df_eob,
            'productOrService',
            pl.col('productOrService').struct.field('coding').list.get(0).struct.field('code'),
            'product_service_code'
        ),
        safe_expr(
            df_eob,
            'productOrService',
            pl.col('productOrService').struct.field('coding').list.get(0).struct.field('system').str.split('/').list.get(-1),
            'product_service_system'
        ),   
        safe_expr(
            df_eob,
            'quantity',
            pl.col('quantity').struct.field('value'),
            'quantity_value'
        ),
        safe_expr(
            df_eob,
            'patient',
            pl.col('patient').struct.field('reference').str.split('/').list.get(1),
            'patient_id'
        ),
        safe_expr(
            df_eob,
            'meta',
            pl.col('meta').struct.field('lastUpdated'),
            'lastUpdated'
        ),
        safe_expr(
            df_eob,
            'revenue',
            pl.col('revenue').struct.field('coding').list.get(0).struct.field('code'),
            'revenue_code'
        ),
        safe_expr(
            df_eob,
            'revenue',
            pl.col('revenue').struct.field('coding').list.get(0).struct.field('display').str.to_titlecase(),
            'revenue_display'
        ),   
        safe_expr(
            df_eob,
            'servicedPeriod',
            pl.col('servicedPeriod').struct.field('start'),
            'service_start'
        ),
        safe_expr(
            df_eob,
            'servicedPeriod',
            pl.col('servicedPeriod').struct.field('end'),
            'service_end'
        ),
        safe_expr(
            df_eob,
            'locationCodeableConcept',
            pl.col('locationCodeableConcept').struct.field('coding').list.get(0).struct.field('code'),
            'location_code'
        ),
        safe_expr(
            df_eob,
            'locationCodeableConcept',
            pl.col('locationCodeableConcept').struct.field('coding').list.get(0).struct.field('display'),
            'location_display'
        ),
        safe_expr(
            df_eob,
            'diagnosisSequence',
            pl.col('diagnosisSequence').list.get(0),
            'diagnosisSequence'
        ),   
        safe_expr(
            df_eob,
            'informationSequence',
            pl.col('informationSequence').list.get(0),
            'informationSequence'
        ),
        safe_expr(
            df_eob,
            'modifier',
            pl.col('modifier').list.get(0).struct.field('coding').list.get(0).struct.field('code'),
            'modifier'
        ),
        safe_expr(
            df_eob,
            'servicedDate',
            pl.when(pl.col('servicedDate') < MIN_SQL_DATE).then(None).otherwise(pl.col('servicedDate')),
            'servicedDate'
        ),
    ]
    df_eob_item = (
        df_eob
        .with_columns(expr)
        .select(column_names)
    )
        
    return df_eob_item.to_pandas()

def process_eob_item_adjudication(df_eob: pl.DataFrame):
    
    column_names = [
            'claim_id',
            'sequence',
            'adjudication_amount',
            'adjudication_category_display',
            'adjudication_reason_display',
            'patient_id',
            'lastUpdated',
            'filename',
            'extract_date'
    ]
    
    df_eob_item_adjudication = (
        df_eob
        .filter(pl.col('adjudication').is_not_null())
        .pipe(flatten, 'adjudication')
        .with_columns(
            pl.lit(now).alias('extract_date')
        )
    )
    
    expres = [
        safe_expr(
            df_eob_item_adjudication,
            'amount',
            pl.col('amount').struct.field('value'),
            'adjudication_amount'
        ),
        safe_expr(
            df_eob_item_adjudication,
            'patient',
            pl.col('patient').struct.field('reference').str.split('/').list.get(1),
            'patient_id'
        ),
        safe_expr(
            df_eob_item_adjudication,
            'meta',
            pl.col('meta').struct.field('lastUpdated'),
            'lastUpdated'
        ),
        safe_expr(
            df_eob_item_adjudication,
            'category',
            pl.col('category').struct.field('coding').list.get(0).struct.field('display'),
            'adjudication_category_display'
        ),
        safe_expr(
            df_eob_item_adjudication,
            'reason',
            pl.col('reason').struct.field('coding').list.get(0).struct.field('display'),
            'adjudication_reason_display'
        ),
    ]
    
    df_eob_item_adjudication = (
        df_eob_item_adjudication
        .with_columns(expres)
        .select(column_names)
    )
    
    return df_eob_item_adjudication.to_pandas()


def process_eob_item_ext(df_eob: pl.DataFrame):
    
    column_names = [
            'claim_id',
            'sequence',
            'valueDecimal',
            'identifierValue',
            'extension_code',
            'extension_display',
            'url',
            'patient_id',
            'lastUpdated',
            'filename',
            'extract_date'
    ]
    
    if 'extension' not in df_eob.columns:
        return pd.DataFrame(columns=column_names)
    
    df_eob_item_ext = (
        df_eob
        .filter(pl.col('extension').is_not_null())
        .pipe(flatten, 'extension')
        .with_columns(
            pl.lit(now).alias('extract_date')
        )
    )
    
    expres = [
        safe_expr(
            df_eob_item_ext,
            'valueCoding',
            pl.col('valueCoding').struct.field('code'),
            'extension_code'
        ),
        safe_expr(
            df_eob_item_ext,
            'valueCoding',
            pl.col('valueCoding').struct.field('display'),
            'extension_display'
        ),
        safe_expr(
            df_eob_item_ext,
            'patient',
            pl.col('patient').struct.field('reference').str.split('/').list.get(1),
            'patient_id'
        ),
        safe_expr(
            df_eob_item_ext,
            'valueIdentifier',
            pl.col('valueIdentifier').struct.field('value'),
            'identifierValue'
        ),
        safe_expr(
            df_eob_item_ext,
            'meta',
            pl.col('meta').struct.field('lastUpdated'),
            'lastUpdated'
        )
    ]
    
    df_eob_item_ext = (
        df_eob_item_ext
        .with_columns(expres)
        .select(column_names)
    )
    
    return df_eob_item_ext.to_pandas()

def process_eob_supporting(df_eob: pl.DataFrame):
    
    column_names = [
            'claim_id',
            'sequence',
            'timingDate',
            'valueString',
            'supportingInfo_category_code',
            'supportingInfo_code',
            'supportingInfo_code_display',
            'supportingInfo_value',
            'supportingInfo_unit',
            'patient_id',
            'lastUpdated',
            'filename',
            'extract_date'
    ]
    
    df_eob_supporting = (
        df_eob
        .filter(pl.col('supportingInfo').is_not_null())
        .select(
            'id',
            'supportingInfo',
            'patient',
            'meta',
            'filename'
        )
        .pipe(flatten, 'supportingInfo')
        .with_columns(
            pl.lit(now).alias('extract_date'),
            pl.col('id').alias('claim_id'),
            pl.col("filename").str.split("/").list.get(-1).alias("filename")
        )
    )
    
    expres = [
        safe_expr(
            df_eob_supporting,
            'valueQuantity',
            pl.col('valueQuantity').struct.field('value'),
            'supportingInfo_value'
        ),
        safe_expr(
            df_eob_supporting,
            'valueQuantity',
            pl.col('valueQuantity').struct.field('unit'),
            'supportingInfo_unit'
        ),
        safe_expr(
            df_eob_supporting,
            'patient',
            pl.col('patient').struct.field('reference').str.split('/').list.get(1),
            'patient_id'
        ),
        safe_expr(
            df_eob_supporting,
            'meta',
            pl.col('meta').struct.field('lastUpdated'),
            'lastUpdated'
        ),
        safe_expr(
            df_eob_supporting,
            'category',
            pl.col('category').struct.field('coding').list.get(0).struct.field('code'),
            'supportingInfo_category_code'
        ),
        safe_expr(
            df_eob_supporting,
            'code',
            pl.col('code').struct.field('coding').list.get(0).struct.field('code'),
            'supportingInfo_code'
        ),
        safe_expr(
            df_eob_supporting,
            'code',
            pl.col('code').struct.field('coding').list.get(0).struct.field('display'),
            'supportingInfo_code_display'
        ),
    ]
    
    df_eob_supporting = (
        df_eob_supporting
        .with_columns(expres)
        .select(column_names)
    )
    
    return df_eob_supporting.to_pandas()


def process_eob_careteam(df_eob: pl.DataFrame):
    
    column_names = [
            'claim_id',
            'sequence',
            'provider_name',
            'npi',
            'taxonomy',
            'specialty_code',
            'role',
            'patient_id',
            'lastUpdated',
            'filename',
            'extract_date'
    ]
    
    df_eob_careteam = (
        df_eob
        .filter(pl.col('careTeam').is_not_null())
        .select(
            'id',
            'careTeam',
            'patient',
            'meta',
            'filename'
        )
        .pipe(flatten, 'careTeam')
        .with_columns(
            pl.lit(now).alias('extract_date'),
            pl.col('id').alias('claim_id'),
            pl.col("filename").str.split("/").list.get(-1).alias("filename")
        )
    )
    
    expres = [
        safe_expr(
            df_eob_careteam,
            'provider',
            pl.col('provider').struct.field('display').str.to_titlecase(),
            'provider_name'
        ),
        safe_expr(
            df_eob_careteam,
            'provider',
            pl.col('provider').struct.field('identifier').struct.field('value'),
            'npi'
        ),
        safe_expr(
            df_eob_careteam,
            'provider',
            pl.col('provider').struct.field('type'),
            'type'
        ),
        safe_expr(
            df_eob_careteam,
            'qualification',
            pl.col('qualification').struct.field('coding').list.get(0).struct.field('code'),
            'specialty_code'
        ),
        safe_expr(
            df_eob_careteam,
            'qualification',
            pl.col('qualification').struct.field('coding').list.get(-1).struct.field('code'),
            'taxonomy'
        ),
        safe_expr(
            df_eob_careteam,
            'role',
            pl.col('role').struct.field('coding').list.get(1).struct.field('code').str.to_titlecase(),
            'role'
        ),
        safe_expr(
            df_eob_careteam,
            'patient',
            pl.col('patient').struct.field('reference').str.split('/').list.get(-1),
            'patient_id'
        ),
        safe_expr(
            df_eob_careteam,
            'meta',
            pl.col('meta').struct.field('lastUpdated'),
            'lastUpdated'
        ),
    ]
    
    df_eob_careteam = (
        df_eob_careteam
        .with_columns(expres)
        .select(column_names)
    )
    
    return df_eob_careteam.to_pandas()


def import_eob_data(file_path: Path = file_path):
    con = None
    try:
        con = duckdb.connect()

        df_eob = con.execute(f"""
            SELECT *
            FROM read_ndjson_auto(
                '{file_path.as_posix()}',
                sample_size=10000,
                filename=true
            )
        """).pl()
        df_item = build_item_df(df_eob)           
        eob_base = process_eob_base(df_eob)
        eob_hemoglobin = process_eob_hemoglobin(df_eob)
        eob_adjudication = process_eob_adjudication(df_eob)
        eob_diagnosis = process_eob_diagnosis(df_eob)
        eob_total = process_eob_total(df_eob)
        eob_procedure = process_eob_procedure(df_eob)
        eob_item = process_eob_item(df_item)
        eob_item_adjudication = process_eob_item_adjudication(df_item)
        eob_item_ext = process_eob_item_ext(df_item)
        eob_supporting = process_eob_supporting(df_eob)
        eob_careteam = process_eob_careteam(df_eob)

        load_to_sql(eob_base, 'eob_base_staging_v3')
        load_to_sql(eob_hemoglobin, 'eob_hemoglobin_staging_v3')
        load_to_sql(eob_adjudication, 'eob_adjudication_staging_v3')
        load_to_sql(eob_diagnosis, 'eob_diagnosis_staging_v3')
        load_to_sql(eob_total, 'eob_total_staging_v3')
        load_to_sql(eob_procedure, 'eob_procedure_staging_v3')
        load_to_sql(eob_item, 'eob_item_staging_v3')
        load_to_sql(eob_item_adjudication, 'eob_item_adjudication_staging_v3')
        load_to_sql(eob_item_ext, 'eob_item_ext_staging_v3')
        load_to_sql(eob_supporting, 'eob_supporting_staging_v3')
        load_to_sql(eob_careteam, 'eob_careteam_staging_v3')

        print(f'{file_path} processed!')
    except Exception as e:
        print(f"\nFAILED: {file_path.name}")
        logging.error(traceback.print_exc())
    finally:
        if con is not None:
            con.close()

def main():
    truncate_tables(table_names)
    
    files = file_path.glob("ExplanationOfBenefit*.ndjson")
    
    with ThreadPoolExecutor(max_workers=2) as executor:
        list(executor.map(import_eob_data, files))
    
    
if __name__ == "__main__":
    main()
