# sqlalchemy-query-utils

Repository: https://github.com/larsderidder/sqlalchemy-query-utils
SQLAlchemy helpers for sorting, filtering, and result shaping.

## Install

```bash
pip install sqlalchemy-query-utils
```

## Usage

```python
from sqlalchemy_query_utils import get_sort_columns, ilike_substr

sort_clause = get_sort_columns({"name": Model.name}, [{"name": "name", "direction": "asc"}])
query = query.order_by(*sort_clause).filter(ilike_substr("gin")(Model.name))
```

## Development

```bash
pip install -e .[dev]
pytest
```
