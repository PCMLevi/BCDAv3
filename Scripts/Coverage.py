import polars as pl
from sqlalchemy import text
from Credentials import engine_DEV_Test as engine
from pathlib import Path
import pandas as pd
import duckdb
from datetime import datetime

file_path = Path(r"C:\BCDA_V3\Data")

now = datetime.now().strftime("%m-%d-%Y %H:%M:%S")


def load_coverage_files(file_path: Path):
    con = duckdb.connect()
    df_coverage = con.execute(
        f'select * from read_ndjson_auto("{file_path}/Coverage*.ndjson", sample_size=-1, union_by_name=true, filename=true)'
    ).pl()
    con.close()
    return df_coverage


def flatten(df: pl.DataFrame, col: str) -> pl.DataFrame:
    return df.explode(col).unnest(col)


def safe_expr(df, source_col, expr, alias):
    if source_col in df.columns:
        return expr.alias(alias)
    return pl.lit(None).alias(alias)


def load_to_sql(df: pd.DataFrame, table_name: str):
    with engine.begin() as conn:
        df.to_sql(table_name, conn, if_exists="replace", index=False)


def process_coverage_basetable(df_coverage: pl.DataFrame):
    

    
    df_coverage_base = (
        df_coverage.select(
            "id",
            "beneficiary",
            "meta",
            "period",
            "payor",
            "status",
            "subscriberId",
            "filename"
        )
        .explode("payor")
        .unnest("payor")
        .unnest("period")
        .with_columns(
            pl.col("beneficiary")
            .struct.field("reference")
            .str.split("/")
            .list.get(1)
            .alias("patient_id"),
            pl.col("meta").struct.field("lastUpdated"),
            pl.lit(now).alias("extract_date"),
            pl.col("filename").str.split("\\").list.get(-1).alias("filename"),
            pl.col("id").alias("coverage_id"),
        )
    )
    if 'end' not in df_coverage_base.columns:
        df_coverage_base = df_coverage_base.with_columns(pl.lit(None).alias('end'))
        
    df_coverage_base = df_coverage_base.select(
        "coverage_id",
        "patient_id",
        "reference",
        "start",
        "end",
        "status",
        "subscriberId",
        "lastUpdated",
        "filename",
        "extract_date",
    )
    
    return df_coverage_base.to_pandas()


def process_coverage_class(df_coverage: pl.DataFrame):
    df_coverage_class = (
        df_coverage.filter(pl.col("class").is_not_null())
        .select("id", "class", "meta", "filename")
        .explode("class")
        .unnest("class")
        .with_columns(
            pl.col("meta").struct.field("lastUpdated"),
            pl.col("type").struct.field("coding").alias("type"),
            pl.lit(now).alias("extract_date"),
            pl.col("filename").str.split("\\").list.get(-1).alias("filename"),
            pl.col("id").alias("coverage_id"),
        )
        .explode("type")
        .unnest("type")
        .select(
            "coverage_id", "code", "value", "lastUpdated", "filename", "extract_date"
        )
    )
    return df_coverage_class.to_pandas()


def process_coverage_extension(df_coverage: pl.DataFrame):

    columns = [
        "coverage_id",
        "url",
        "valueString",
        "valueDecimal",
        "code",
        "lastUpdated",
        "filename",
        "extract_date",
    ]

    df_coverage_extension = (
        df_coverage.filter(pl.col("extension").is_not_null())
        .select("id", "extension", "meta", "filename")
        .explode("extension")
        .unnest("extension")
        .with_columns(
            pl.col("meta").struct.field("lastUpdated"),
            pl.col("valueCoding").struct.field("code").alias("code"),
            pl.col("valueCoding").struct.field("system").alias("system"),
            pl.col("filename").str.split("\\").list.get(-1).alias("filename"),
            pl.col("id").alias("coverage_id"),
            pl.lit(now).alias("extract_date"),
        )
    )
    if "valueDecimal" in df_coverage_extension.columns:
        df_coverage_extension = df_coverage_extension.select(columns)
    elif "valueDecimal" not in df_coverage_extension.columns:
        df_coverage_extension = df_coverage_extension.with_columns(
            pl.lit(None).alias("valueDecimal")
        ).select(columns)
    return df_coverage_extension.to_pandas()


def process_coverage_contained(df_coverage: pl.DataFrame):
    column_names = [
        "coverage_id",
        "active",
        "contained_id",
        "name",
        "resourceType",
        "lastUpdated",
        "filename",
    ]

    df_coverage_contained = (
        df_coverage.filter(pl.col("contained").is_not_null())
        .with_columns(
            pl.col("id").alias("coverage_id"), pl.col("meta").alias("meta_main")
        )
        .select("coverage_id", "contained", "meta_main", "filename")
        .explode("contained")
        .unnest("contained")
        .with_columns(
            pl.col("id").alias("contained_id"),
            pl.col("meta_main").struct.field("lastUpdated"),
            pl.col("filename").str.split("\\").list.get(-1).alias("filename"),
            pl.col("name").str.to_titlecase().alias("name"),
            pl.lit(now).alias("extract_date"),
        )
        .select(column_names)
    )
    return df_coverage_contained.to_pandas()


def import_coverage_data(file_path: Path = file_path):
    df_coverage = load_coverage_files(file_path)

    coverage_base = process_coverage_basetable(df_coverage)
    coverage_class = process_coverage_class(df_coverage)
    coverage_extension = process_coverage_extension(df_coverage)
    coverage_contained = process_coverage_contained(df_coverage)

    load_to_sql(coverage_base, "coverage_base_staging_v3")
    load_to_sql(coverage_class, "coverage_class_staging_v3")
    load_to_sql(coverage_extension, "coverage_extension_staging_v3")
    load_to_sql(coverage_contained, "coverage_contained_staging_v3")


def main():
    if any(Path(r'C:\BCDA_V3\Data').glob('Coverage*.ndjson')):
        import_coverage_data(file_path)


if __name__ == "__main__":
    main()
