
-- 2. PREPARE FOR AGGREGATION
CREATE temp TABLE aggregation_data AS
SELECT a.ACH_DATE, c.PCN_CD, a.GP_CD, b.PCG_CD as IND_CD, a.NUM, a.DEN, a.PCA
from pivot_data as a left join scheme_info as b on a.IND_CD = b.NCD_CD
left join organisation as c on a.GP_CD = c.GP_CD;

create TEMP TABLE gp_head AS
SELECT a.ACH_DATE, b.GP_CD, c.IND_CD 
from 
(SELECT distinct ACH_DATE from pivot_data) as a 
CROSS JOIN (select distinct GP_CD from organisation) as b 
CROSS join (select distinct PCG_CD as IND_CD from scheme_info) as c;

create TEMP TABLE pcn_head AS
SELECT a.ACH_DATE, b.PCN_CD, c.IND_CD 
from 
(SELECT distinct ACH_DATE from aggregation_data) as a 
CROSS JOIN (select distinct PCN_CD from organisation) as b 
CROSS join (select distinct PCG_CD as IND_CD from scheme_info) as c;

-- 3. GP AGGREGATION
DROP TABLE if EXISTS fact_gpdata;
CREATE TABLE fact_gpdata as 
SELECT a.ACH_DATE, a.GP_CD, a.IND_CD, b.NUM, b.DEN, b.PCA, 
case when b.DEN is null then "FALSE" else "TRUE" end as SUBMIT_STATUS
from gp_head as a 
left join aggregation_data as b on a.ACH_DATE = b.ACH_DATE and a.GP_CD = b.GP_CD and a.IND_CD = b.IND_CD;

-- 4. PCN AGGREGATION
DROP TABLE if EXISTS supp_pcn_data;
CREATE TABLE supp_pcn_data as 
SELECT a.ACH_DATE, a.PCN_CD, a.IND_CD, b.NUM, b.DEN, b.PCA, 
case when b.DEN is null then "FALSE" else "TRUE" end as SUBMIT_STATUS
from pcn_head as a 
left join 
(
select ACH_DATE, PCN_CD, IND_CD, sum(NUM) as NUM, sum(DEN) as DEN, sum(PCA) as PCA 
from aggregation_data group by ACH_DATE, PCN_CD, IND_CD
) as b on a.ACH_DATE = b.ACH_DATE and a.PCN_CD = b.PCN_CD and a.IND_CD = b.IND_CD;

