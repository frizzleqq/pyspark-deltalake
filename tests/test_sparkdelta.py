import pytest
from delta import DeltaTable

import sparkdelta

# To avoid conflicts between tests, the table name is based on the test name
# by using fixtures.


def test_create_table_if_not_exists(spark, tmp_path, schema_name, table_name):
    sparkdelta.create_schema_if_not_exists(schema_name)
    df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["col1", "col2"])
    sparkdelta.create_table_if_not_exists(schema_name, table_name, df, tmp_path.as_uri())

    delta_table = DeltaTable.forName(
        spark, sparkdelta.get_full_table_name(schema_name, table_name)
    )
    assert delta_table.toDF().count() == 0
    assert delta_table.toDF().columns == ["col1", "col2"]

    details = sparkdelta.get_table_details(schema_name, table_name)
    assert details["format"] == "delta"
    assert details["partitionColumns"] == []
    assert details["clusteringColumns"] == []


def test_create_table_if_not_exists_partition_and_cluster(
    spark, tmp_path, schema_name, table_name
):
    sparkdelta.create_schema_if_not_exists(schema_name)
    df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["col1", "col2"])
    with pytest.raises(ValueError, match="either partition_by or cluster_by"):
        sparkdelta.create_table_if_not_exists(
            schema_name,
            table_name,
            df,
            tmp_path.as_uri(),
            partition_by=["col1"],
            cluster_by=["col2"],
        )


def test_create_table_if_not_exists_partition_by(
    spark, tmp_path, schema_name, table_name
):
    sparkdelta.create_schema_if_not_exists(schema_name)
    df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["col1", "col2"])
    sparkdelta.create_table_if_not_exists(
        schema_name, table_name, df, tmp_path.as_uri(), partition_by=["col1"]
    )

    delta_table = DeltaTable.forName(
        spark, sparkdelta.get_full_table_name(schema_name, table_name)
    )
    assert delta_table.toDF().count() == 0
    assert delta_table.toDF().columns == ["col1", "col2"]

    details = sparkdelta.get_table_details(schema_name, table_name)
    assert details["format"] == "delta"
    assert details["partitionColumns"] == ["col1"]


def test_create_table_if_not_exists_clusterby(spark, tmp_path, schema_name, table_name):
    sparkdelta.create_schema_if_not_exists(schema_name)
    df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["col1", "col2"])
    sparkdelta.create_table_if_not_exists(
        schema_name, table_name, df, tmp_path.as_uri(), cluster_by=["col1"]
    )

    delta_table = DeltaTable.forName(
        spark, sparkdelta.get_full_table_name(schema_name, table_name)
    )
    assert delta_table.toDF().count() == 0
    assert delta_table.toDF().columns == ["col1", "col2"]

    details = sparkdelta.get_table_details(schema_name, table_name)
    assert details["format"] == "delta"
    assert details["clusteringColumns"] == ["col1"]


def test_write(spark, tmp_path, schema_name, table_name):
    sparkdelta.create_schema_if_not_exists(schema_name)
    df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["col1", "col2"])
    sparkdelta.write(schema_name, table_name, df, tmp_path.as_uri())

    delta_table = DeltaTable.forName(
        spark, sparkdelta.get_full_table_name(schema_name, table_name)
    )
    assert delta_table.toDF().count() == 3
    assert delta_table.toDF().columns == ["col1", "col2"]
    details = sparkdelta.get_table_details(schema_name, table_name)
    assert details["format"] == "delta"


def test_get_last_delta_operation(spark, tmp_path, schema_name, table_name):
    sparkdelta.create_schema_if_not_exists(schema_name)
    df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["col1", "col2"])
    sparkdelta.create_table_if_not_exists(schema_name, table_name, df, tmp_path.as_uri())

    result = sparkdelta.get_last_delta_operation(schema_name, table_name)
    assert result["version"] == 0
    assert result["operation"] == "CREATE TABLE"


def test_get_last_delta_operation_after_write(spark, tmp_path, schema_name, table_name):
    sparkdelta.create_schema_if_not_exists(schema_name)
    df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["col1", "col2"])
    result = sparkdelta.write(schema_name, table_name, df, tmp_path.as_uri())

    # version 1 as creation is a separate operation in 'create_table_if_not_exists'
    assert result["version"] == 1
    assert result["operation"] == "WRITE"
