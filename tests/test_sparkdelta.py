from delta import DeltaTable

import sparkdelta

# To avoid conflicts between tests, the schema name is based on the test name
# by using the request fixture.


def test_write(spark, request, tmp_path):
    test_schema = f"schema_{request.node.name}"
    sparkdelta.create_schema_if_not_exists(test_schema)
    df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["col1", "col2"])
    sparkdelta.write(test_schema, "test_table", df, tmp_path.as_uri())

    delta_table = DeltaTable.forName(spark, f"{test_schema}.test_table")
    assert delta_table.toDF().count() == 3
    assert delta_table.toDF().columns == ["col1", "col2"]
    assert delta_table.detail().first().asDict()["format"] == "delta"


def test_get_last_delta_operation(spark, request, tmp_path):
    test_schema = f"schema_{request.node.name}"
    sparkdelta.create_schema_if_not_exists(test_schema)
    df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["col1", "col2"])
    result = sparkdelta.write(test_schema, "test_table", df, tmp_path.as_uri())

    # version 1 as creation is a separate operation in 'create_table_if_not_exists'
    assert result["version"] == 1
    assert result["operation"] == "WRITE"
