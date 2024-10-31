from collections import namedtuple

from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base

from sqlalchemy_query_utils import (
    check_number,
    compare_number,
    entity_to_dict,
    get_sort_columns,
    group_result_set,
    ilike_substr,
    set_attribute,
)

Base = declarative_base()


class Item(Base):
    __tablename__ = "items"

    id = Column(Integer, primary_key=True)
    name = Column(String)


def test_get_sort_columns():
    mapping = {"name": Item.name, "id": Item.id}
    sort_order = [{"name": "name", "direction": "asc"}]
    columns = get_sort_columns(mapping, sort_order)
    assert columns


def test_set_attribute():
    entity = {"a": 1}
    set_attribute(entity, "b__c", 3)
    assert entity["b"]["c"] == 3


def test_entity_to_dict():
    Row = namedtuple("Row", ["foo", "bar__baz"])
    row = Row(foo=1, bar__baz=2)
    result = entity_to_dict(row)
    assert result["foo"] == 1
    assert result["bar"]["baz"] == 2


def test_group_result_set():
    Row = namedtuple("Row", ["id", "value"])
    rows = [Row(1, "a"), Row(1, "b"), Row(2, "c")]
    grouped = group_result_set(rows)
    assert grouped[0]["value"] == ["a", "b"]
    assert grouped[1]["value"] == "c"


def test_ilike_substr():
    clause = ilike_substr("abc")(Item.name)
    assert clause is not None


def test_check_number():
    expr = check_number(Item.id, "10")
    assert expr is not False
    expr_float = compare_number(Item.id, 10.0)
    assert expr_float is not False
