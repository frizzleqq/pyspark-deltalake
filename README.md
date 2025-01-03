# Local env with spark+delta

Minimal example of a local Python setup with Spark and DeltaLake that allows
unit-testing spark/delta via `pytest`.

The setup is inspired by [dbx by Databricks](https://github.com/databrickslabs/dbx).

## Delta

To include Delta in the Spark session created by `pytest` the `spark` fixture in
[./tests/conftest.py](./tests/conftest.py) runs `configure_spark_with_delta_pip` and adds the following settings to the spark config:

| key | value |
| - | - |
| spark.sql.extensions | io.delta.sql.DeltaSparkSessionExtension
| spark.sql.catalog.spark_catalog | org.apache.spark.sql.delta.catalog.DeltaCatalog

See https://docs.delta.io/3.2.0/quick-start.html#python for more info.


## Development

Requirements:
* Python >= 3.10
* Java 8, 11 or 17 for Spark (https://spark.apache.org/docs/3.5.1/#downloading)
  * `JAVA_HOME` must be set

### Setup Virtual environment

Following commands create and activate a virtual environment.
* The `[dev]` also installs development tools.
* The `--editable` makes the CLI script available.

Commands:
* Makefile:
    ```bash
    make requirements
    source .venv/bin/activate
    ```
* Windows:
    ```powershell
    python -m venv .venv
    .venv\Scripts\activate
    python -m pip install --upgrade uv
    uv pip install --editable .[dev]
    ```

### Updating locked dependencies

To lock dependencies from `pyproject.toml` into `requirements.txt` files:

* Without dev dependencies:
    ```
    uv pip compile pyproject.toml --upgrade -o requirements.txt
    ```

* With dev dependencies:
    ```
    uv pip compile pyproject.toml --upgrade --extra dev -o requirements-dev.txt
    ```
  * We use `uv pip install` instead of `uv pip sync` to also have an editable install.

### Windows

**I recommend using [wsl](https://learn.microsoft.com/en-us/windows/wsl/install) instead,
as even with the additional hadoop libraries spark-delta occasionally simply freezes.**

To run this on Windows you need additional Haddop libraries, see https://cwiki.apache.org/confluence/display/HADOOP2/WindowsProblems.

> "In particular, %HADOOP_HOME%\BIN\WINUTILS.EXE must be locatable."

1. Download the `bin` directory https://github.com/steveloughran/winutils/tree/master/hadoop-3.0.0/bin (required files: `hadoop.dll` and `winutils.exe`)
2. Set environment variable `HADOOP_HOME` to the directory above the `bin` directory

### Run tests

* Makefile
```bash
make test
```

* Windows
```
pytest -vv
```
