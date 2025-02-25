-- SQLID:SQL001
SQL001 =
select * from hog_table order by id, name

-- SQLID:SQL002
SQL002 =
select entity_id, str from dialect where str like ? order by entity_id