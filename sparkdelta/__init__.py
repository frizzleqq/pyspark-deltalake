from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def get_full_table_name(schema_name: str, table_name: str) -> str:
    """
    Get full table name with schema.

    Parameters
    ----------
        schema_name : str
            schema name of table
        table_name : str
            table name

    Returns
    -------
    str
        full table name with schema
    """
    return f"{schema_name}.{table_name}"


def prepare_spark(spark: SparkSession | None = None) -> SparkSession:
    """
    Get or prepare SparkSession.
    Inspired by https://github.com/databrickslabs/dbx

    Parameters
    ----------
    spark : SparkSession, optional
        spark session, by default None

    Returns
    -------
    SparkSession
        spark session
    """
    if not spark:
        return SparkSession.builder.getOrCreate()
    else:
        return spark


def create_schema_if_not_exists(schema_name: str) -> None:
    """
    Create spark schema (aka database) if it does not exist.

    Parameters
    ----------
        schema_name : str
            schema name of table
    """
    prepare_spark().sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")


def create_table_if_not_exists(
    schema_name: str, table_name: str, df: DataFrame, location: str
) -> DeltaTable:
    """
    Create Delta Lake table if it does not exist yet.

    Parameters
    ----------
        schema_name : str
            schema name of table
        table_name : str
            table name
        df : DataFrame
            DataFrame to write
        location : str
            location of table

    Returns
    -------
    DeltaTable
        DeltaTable object of created table
    """
    table_builder = (
        DeltaTable.createIfNotExists(prepare_spark())
        .tableName(get_full_table_name(schema_name, table_name))
        .location(location)
        .addColumns(df.schema)
    )
    return table_builder.execute()


def get_last_delta_operation(schema_name: str, table_name: str) -> dict:
    """
    Get latest Delta Table history entry.

    Parameters
    ----------
        schema_name : str
            schema name of table
        table_name : str
            table name

    Returns
    -------
    dict
        latest delta history entry
    """
    delta_table = DeltaTable.forName(
        prepare_spark(), get_full_table_name(schema_name, table_name)
    )
    latest_row = (
        delta_table.history(1)
        .select("version", "timestamp", "operation", "operationMetrics")
        .sort("version", ascending=False)
        .first()
    )
    if latest_row:
        return latest_row.asDict()
    else:
        return {}


def write(
    schema_name: str,
    table_name: str,
    df: DataFrame,
    location: str,
    mode: str = "append",
) -> dict:
    """
    Write DataFrame to Delta Lake table.

    Parameters
    ----------
        schema_name : str
            schema name of table
        table_name : str
            table name
        df : DataFrame
            DataFrame to write
        mode : str, optional
            write mode, by default "append"

    Returns
    -------
    dict
        latest delta history entry
    """
    create_table_if_not_exists(schema_name, table_name, df, location)
    df.write.format("delta").mode(mode).saveAsTable(
        get_full_table_name(schema_name, table_name)
    )
    return get_last_delta_operation(schema_name, table_name)
