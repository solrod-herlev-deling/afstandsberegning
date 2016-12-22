-- select the ways.the_geom (road segment) that is closest to the bus stop point.
-- include values from sequence (has to be unique from the ogc_fid used in the nearest_ways_borger_aktiv below, 
-- so the sequence starts at max(geo_borger_aktiv.ogc_fid)). All ogc_fid->id are cast as negative 
-- integer to conform to the pg_withPointsCost function.
-- LineLocatePoint is used to get at which fraction of the closest line the poi is located.
-- DROP TABLE base_data.nearest_ways_busstop 
CREATE TABLE base_data.nearest_ways_busstop as
  WITH ve AS (
  select distinct on(akt.gid) akt.gid, nextval('base_data.seq_busstop') as id, ver.gid as edge_id

   , ST_LineLocatePoint(ver.the_geom,st_transform(akt.geom,4326)) as fraction, 
   ver.source, ver.target
from osm_network.herlev_ways ver, base_data.stoppesteder_beskaaret akt
where st_dwithin(ver.the_geom, st_transform(akt.geom,4326), 0.001) 
order by gid, st_distance(ver.the_geom, st_transform(akt.geom,4326)) ASC)
SELECT gid,id, 
   CASE WHEN fraction = 0 THEN source
        WHEN fraction = 1 THEN target 
        ELSE -id 
        END AS pid,
   edge_id, fraction
FROM ve
;
-- SELECT * FROM base_data.nearest_ways_busstop WHERE pid > 0
------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------
-- same as above. No sequence needed though
-- DROP TABLE base_data.nearest_ways_geo_bbr_adgangsadresse_walk
CREATE TABLE base_data.nearest_ways_geo_bbr_adgangsadresse_walk as
  WITH ve AS (
  select distinct on(ogc_fid) ogc_fid as id, ver.gid as edge_id

   , ST_LineLocatePoint(ver.the_geom,st_transform(akt.wkb_geometry,4326)) as  fraction, 
   ver.source, ver.target, st_distance(st_transform(ver.the_geom,25832), akt.wkb_geometry) as dist
from osm_network.herlev_ways ver, base_data.geo_bbr_adgangsadresse akt
where st_dwithin(ver.the_geom, st_transform(akt.wkb_geometry,4326), 0.01)
and class_id > 105 
order by ogc_fid, st_distance(ver.the_geom, st_transform(akt.wkb_geometry,4326)) ASC
)
SELECT id, dist,
   CASE WHEN fraction = 0 THEN source
        WHEN fraction = 1 THEN target 
        ELSE -id 
        END AS pid,
   edge_id, fraction
FROM ve
;

-- select setval('base_data.seq_busstop', max(pid)) from base_data.nearest_ways_geo_bbr_adgangsadresse where pid > 0
------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------

ALTER TABLE base_data.nearest_ways_busstop
ADD COLUMN nearest_vertice bigint;

with cte as(
  select distinct on(akt.gid) akt.gid, id

   
from osm_network.ways_vertices_pgr ver, base_data.stoppesteder_beskaaret akt
where st_dwithin(ver.the_geom, st_transform(akt.geom,4326), 0.001) 
order by gid, st_distance(ver.the_geom, st_transform(akt.geom,4326)) ASC
)
update base_data.nearest_ways_busstop
set nearest_vertice =
 (select id from cte
where cte.gid=nearest_ways_busstop.gid)
;

------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------

-- CREATE INDEX nearest_ways_geo_borger_aktiv_edge_id_idx
--   ON base_data.nearest_ways_geo_borger_aktiv
--   USING btree
--   (edge_id);
-- 
-- CREATE INDEX nearest_ways_busstop_edge_id_idx
--   ON base_data.nearest_ways_busstop
--   USING btree
--   (edge_id);
-- 
-- CREATE INDEX nearest_ways_geo_bbr_adgangsadresse_walk_pid_idx
--   ON base_data.nearest_ways_geo_bbr_adgangsadresse_walk
--   USING btree
--   (pid);
-- 
-- CREATE INDEX nearest_ways_busstop_pid_idx
--   ON base_data.nearest_ways_busstop
--   USING btree
--   (pid);
-- 

-- 
-- CREATE INDEX geo_bbr_adgangsadresseogc_fid_idx
--   ON base_data.geo_bbr_adgangsadresse 
--   USING btree
--   (ogc_fid);
-- 
-- ------------------------------------------------------------------------------------------------------------------------------------
-- ------------------------------------------------------------------------------------------------------------------------------------
-- 
-- 
-- CREATE UNIQUE INDEX geo_borger_aktiv_un_ogc_fid_idx
--   ON base_data.geo_borger_aktiv
--   USING btree
--   (ogc_fid);
-- 
-- 
-- CREATE UNIQUE INDEX nearest_ways_geo_borger_aktiv_un_pid_idx
--   ON base_data.nearest_ways_geo_borger_aktiv
--   USING btree 
--   (pid) where pid < 0;
-- 
-- CREATE UNIQUE INDEX nearest_ways_busstop_un_pid_idx
--   ON base_data.nearest_ways_busstop
--   USING btree
--   (pid);
-- 
-- 

------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------
-- pgr_withPointsCost takes as its first argument a select of all unique ways.gids (as id), as well as corresponding source,target and cost.
-- The where clause is to avoid road classes (class_id) that where pedestrians are not permitted.
-- Second argument is all poi (both bus stops and borgere via UNION) with unique pid (*-1 to get positive), nearest edge_id and 
-- fraction.
-- Third and fourth argument is the arrays of respectively bus stop and borgere pid's.
-- To only get the minimum cost (to the nearest bus stop) for each borger, we use SELECT DISTINCT ON on end_pid (unique for borger).
-- Finally join the approiate geometry back on to the result via absolute end_pid->ogc_fid.
-- drop table base_data.min_walk_cost_to_bus_stop 
create table base_data.min_walk_cost_to_bus_stop as
with cost_all as (
SELECT * 
  FROM pgr_withPointsCost('select gid as id, source, target, 
    length_m as cost, length_m as reverse_cost 
    from osm_network.ways
    where class_id > 105',
-- we want only virtual nodes (those not in our graph) (and make node id +)
'select 

CASE WHEN fraction = 0 THEN pid
        WHEN fraction = 1 THEN pid 
        ELSE -pid 
        END AS pid


, edge_id, fraction 
from base_data.nearest_ways_geo_bbr_adgangsadresse_walk
where pid < 0
union all
select -pid AS pid, edge_id, fraction 
from base_data.nearest_ways_busstop',
(select array_agg(nearest_ways_busstop.pid) from base_data.nearest_ways_busstop  ),
 
 (select array_agg(nearest_ways_geo_bbr_adgangsadresse_walk.pid) from base_data.nearest_ways_geo_bbr_adgangsadresse_walk ) 
 )
UNION ALL

SELECT * FROM pgr_dijkstraCost('select gid as id, source, target, 
    length_m as cost, length_m as reverse_cost 
    from osm_network.ways
    where class_id > 105',
(select array_agg(nearest_ways_busstop.nearest_vertice) from base_data.nearest_ways_busstop  ),
 
 (select array_agg(nearest_ways_geo_bbr_adgangsadresse_walk.pid) from base_data.nearest_ways_geo_bbr_adgangsadresse_walk where pid > 0 ) 
 )
 )


select distinct on (end_pid) start_pid, end_pid, agg_cost + dist as min_cost, kvh_adr_key, wkb_geometry from cost_all
left join base_data.geo_bbr_adgangsadresse on abs(end_pid)=ogc_fid
left join base_data.nearest_ways_geo_bbr_adgangsadresse_walk on abs(end_pid)=id
order by end_pid, min_cost
;
-- Query returned successfully: 26241 rows affected, 01:00:3647 hours execution time.
-- use CREATE TEMP TABLE followed by an index, then join
-- Add dist from nearest_ways_geo_borger_aktiv to final min_cost
-- Add primary keys to tables?
