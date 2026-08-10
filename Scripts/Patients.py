import polars as pl
from sqlalchemy import text
from Credentials import engine_DEV_Test as engine
from pathlib import Path
import pandas as pd
import duckdb
from datetime import datetime

file_path = Path(r"C:\BCDA_V3\Data")

now = datetime.now().strftime("%m-%d-%Y %H:%M:%S")


def load_patient_files(file_path: Path):
    con = duckdb.connect()
    df_patient = con.execute(
        f'select * from read_ndjson_auto("{file_path}/Patient*.ndjson", sample_size=-1, union_by_name=true, filename=true)'
    ).pl()
    con.close()
    return df_patient


def load_to_sql(df: pd.DataFrame, table_name: str):
    with engine.begin() as conn:
        df.to_sql(table_name, conn, if_exists="replace", index=False)


def flatten(df: pl.DataFrame, col: str) -> pl.DataFrame:
    return df.explode(col).unnest(col)


def process_patient_basetable(df_patient: pl.DataFrame):

    columns = [
        "patient_id",
        "id_link",
        "family",
        "given",
        "gender",
        "race",
        "birthDate",
        "postalCode",
        "state",
        "deceasedDateTime",
        "lastUpdated",
        "filename",
        "extract_date",
    ]

    df_patient_race = (
        df_patient.select(["id", "extension"])
        .pipe(flatten, "extension")
        .select(["id", "extension"])
        .pipe(flatten, "extension")
        .select(["id", "valueString"])
        .filter(pl.col("valueString").is_not_null())
    )

    patient_base = (
        df_patient.pipe(flatten, "address")
        .pipe(flatten, "name")
        # .pipe(flatten, 'link')
        .with_columns(
            pl.col("given").list.join(", ").str.to_titlecase(),
            pl.col("family").str.to_titlecase().alias("family"),
            pl.col("meta").struct.field("lastUpdated"),
            # pl.col('other').struct.field('display').alias('id_link'),
            pl.col("filename").str.split("\\").list.get(-1).alias("filename"),
            pl.col("id").alias("patient_id"),
        )
        .join(df_patient_race.select(["id", "valueString"]), on="id", how="left")
    )
    if "deceasedDateTime" not in patient_base.columns:
        patient_base = patient_base.with_columns(pl.lit(None).alias("deceasedDateTime"))

    if "link" in patient_base.columns:
        patient_base_sorted = patient_base.with_columns(
            pl.col("link")
            .list.get(0)
            .struct.field("other")
            .struct.field("display")
            .alias("id_link"),
            pl.col("valueString").alias("race"),
            pl.lit(now).alias("extract_date"),
        ).select(columns)
    elif "link" not in patient_base.columns:
        patient_base_sorted = patient_base.with_columns(
            pl.lit(None).alias("id_link"),
            pl.col("valueString").alias("race"),
            pl.lit(now).alias("extract_date"),
        ).select(columns)
    return patient_base_sorted.to_pandas()


def process_patient_linktable(df_patient: pl.DataFrame):
    patient_linktable = (
        df_patient.filter(pl.col("identifier").is_not_null())
        .select("id", "identifier", "filename", "meta")
        .pipe(flatten, "identifier")
        .select("id", "period", "value", "filename", "meta")
        .unnest("period")
        .with_columns(
            pl.col("meta").struct.field("lastUpdated"),
            pl.col("filename").str.split("\\").list.get(-1).alias("filename"),
            pl.col("id").alias("patient_id"),
            pl.lit(now).alias("extract_date"),
        )
        .select(
            "patient_id",
            "start",
            "end",
            "value",
            "lastUpdated",
            "filename",
            "extract_date",
        )
    )
    return patient_linktable.to_pandas()


def import_patient_data(file_path: Path = file_path):
    df_patient = load_patient_files(file_path)

    patient_base_sorted = process_patient_basetable(df_patient)
    patient_linktable = process_patient_linktable(df_patient)

    load_to_sql(patient_base_sorted, "patient_base_staging_v3")
    load_to_sql(patient_linktable, "patient_link_staging_v3")


def main():
    if any(Path(r'C:\BCDA_V3\Data').glob('Patient*.ndjson')):
        import_patient_data(file_path)


if __name__ == "__main__":
    main()
