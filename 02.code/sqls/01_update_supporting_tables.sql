-- SQLite
-- NB assumes that the update of the bulk extract has been already performed since
-- it relies on identification of the month of update of the report


-- 1. CREATE ORGANISATION
-- a) create prep tables
create temp TABLE report_update AS
select distinct ACH_DATE from bulk ORDER by ACH_DATE desc limit 1;

create temp table active_pcns as 
select * from pcns 
where Close is NULL OR Close >= (select * from report_update limit 1);

create temp table active_gps as
select * from gps 
where StartRel <= (select * from report_update limit 1) 
and (EndRel is NULL OR EndRel >= (select * from report_update limit 1));

CREATE TABLE orgs_prep as 
SELECT a.SUBICB_CD,a.SUBICB_NM,a.PCN_CD, a.PCN_NM,b.GP_CD,b.GP_NM 
from active_pcns as a left join active_gps as b on a.PCN_CD = b.PCN_CD;

-- b) create organisation
DROP TABLE if EXISTS organisation;
CREATE TABLE organisation AS
SELECT null as LAST_UPDATE, b.REG_CD, b.REG_NM, b.ICB_CD, b.ICB_NM, a.*
from orgs_prep as a LEFT JOIN 
(select distinct SUBICB_CD,SUBICB_NM, ICB_CD, ICB_NM, REG_CD, REG_NM from org ) as b 
on a.SUBICB_CD = b.SUBICB_CD;
UPDATE organisation set LAST_UPDATE = (select MAX(StartRel) from gps);

-- 2. CREATE LIST
DROP TABLE IF EXISTS list; 
CREATE TABLE list as 
SELECT null as LAST_UPDATE, a.*, b.PATS as LIST
from orgs_prep as a left join pats as b on a.GP_CD = b.GP_CD;
UPDATE list set LAST_UPDATE = (SELECT DISTINCT EXTRACT from pats);

-- 3. DROP INPUT TABLES
DROP TABLE if EXISTS report_update;
DROP TABLE if EXISTS orgs_prep;
DROP TABLE if EXISTS gps;
DROP TABLE if EXISTS pcns;
DROP TABLE if EXISTS pats;
DROP TABLE if EXISTS org;
