select q.*, rowNumberInAllBlocks()+1 AS "rank" from 
(select 
t.`User`, t.Value
from dbo.table1 t 
order by t.Value desc)q
