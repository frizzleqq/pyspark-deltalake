PACKAGE := sparkdelta

SHELL=/bin/bash
VENV=.venv

ifeq ($(OS),Windows_NT)
	VENV_BIN=$(VENV)/Scripts
else
	VENV_BIN=$(VENV)/bin
endif

.venv:  ## Set up Python virtual environment and install requirements
	python -m venv $(VENV)
	$(MAKE) requirements

.PHONY: requirements
requirements: .venv  ## Install/refresh Python project requirements
	$(VENV_BIN)/python -m pip install --upgrade uv
	$(VENV_BIN)/uv pip install -r requirements-dev.txt -e .

.PHONY: build
build:
	$(VENV_BIN)/uv pip install build
	$(VENV_BIN)/python -m build

.PHONY: format
format:
	$(VENV_BIN)/ruff check $(PACKAGE) tests --fix
	$(VENV_BIN)/ruff format $(PACKAGE) tests

.PHONY: lint
lint:
	$(VENV_BIN)/ruff check $(PACKAGE)
	$(VENV_BIN)/ruff format $(PACKAGE) --check
	$(VENV_BIN)/mypy $(PACKAGE)

.PHONY: pip-compile
pip-compile:
	$(VENV_BIN)/uv pip compile pyproject.toml --upgrade -o requirements.txt
	$(VENV_BIN)/uv pip compile pyproject.toml --upgrade --extra dev -o requirements-dev.txt

.PHONY: test
test:
	$(VENV_BIN)/pytest -vv
