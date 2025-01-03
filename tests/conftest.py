import logging
import os
import shutil
import sys
from typing import Generator

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# Ensure Spark uses the same Python executable as pytest
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


@pytest.fixture(scope="session")
def spark(
    tmp_path_factory: pytest.TempPathFactory,
) -> Generator[SparkSession, None, None]:
    """
    This fixture provides preconfigured SparkSession with Hive and Delta support.
    After the test session, temporary warehouse directory is deleted.

    Based on:
    * dbx: https://github.com/databrickslabs/dbx
    * delta: https://github.com/delta-io/delta/blob/master/python/delta/testing/utils.py

    Returns
    -------
        SparkSession: preconfigured SparkSession
    """
    logging.info("Configuring Spark session for testing environment")
    # use pytest tmp_path to keep everything together
    warehouse_dir = tmp_path_factory.getbasetemp().joinpath("warehouse")
    _builder = (
        SparkSession.builder.master("local[*]")
        .config("spark.hive.metastore.warehouse.dir", warehouse_dir.as_uri())
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # https://github.com/delta-io/delta/blob/master/python/delta/testing/utils.py
        .config("spark.ui.enabled", "false")
        .config("spark.databricks.delta.snapshotPartitions", 2)
        .config("spark.sql.shuffle.partitions", 5)
        .config("delta.log.cacheSize", 3)
        .config("spark.databricks.delta.delta.log.cacheSize", 3)
        .config("spark.sql.sources.parallelPartitionDiscovery.parallelism", 5)
    )
    spark: SparkSession = configure_spark_with_delta_pip(_builder).getOrCreate()
    logging.info("Spark session configured")
    yield spark
    logging.info("Shutting down Spark session")
    spark.stop()
    if warehouse_dir.exists():
        shutil.rmtree(warehouse_dir)


@pytest.fixture(scope="module")
def schema_name(request) -> str:
    """
    This fixture provides schema name for testing environment.
    `request.module.__name__` is the name of the module of the test function.

    Returns
    -------
        str: module name of test
    """
    return request.module.__name__


@pytest.fixture(scope="function")
def table_name(request) -> str:
    """
    This fixture provides table name for testing environment.
    `request.node.name` is the name of the test function.

    Returns
    -------
        str: test name
    """
    return request.node.name
