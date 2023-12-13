-- SQLite


-- 1. CREATE BASE DATASET
-- a. Extract NUM,DEN,PCA and relevant fields only
create temp TABLE measure_data01 AS
SELECT a.ACH_DATE, a.ORG_CODE, a.IND_CODE, b.MEASURE_TYPE, a.VALUE 
from bulk as a left join measures as b on a.IND_CODE = b.IND_CODE and a.FIELD_NAME = b.MEASURE 
where 
(a.LAST_SUBMISSION is 'Y' or a.SUBMIT_USER_ID is 'cro-processor')  
and b.MEASURE_TYPE in ('NUM','DEN','PCA') 
-- b. add patch for wrong ACC-08 data sources 
and b.MEASURE is not 'Number of appointments provided by the general practice that were mapped to one of the following eight national categories: - General Consultation Acute - General Consultation Routine - Unplanned Clinical Activity - Walk in - Triage  Home Visit - Care Home Visit - Care Related Encounter but does not fit into any other category.'; 
-- c. update data assuming CAN-01 and HI-01 are comparable to CAN-02 and HI-03
UPDATE measure_data01 set IND_CODE = 'NCD120' where IND_CODE = 'NCD005' and ACH_DATE <=20230331;
UPDATE measure_data01 set IND_CODE = 'NCD123' where IND_CODE = 'NCD112' and ACH_DATE <=20230331;
DELETE from measure_data01 where IND_CODE not in ('NCD026','NCD120','NCD123','NCD003','NCD004');
-- e. define last 2 months
CREATE TEMP TABLE dates_filter01 as
select distinct ACH_DATE from measure_data01 ORDER by ACH_DATE desc limit 2;
create TEMP TABLE dates_filter AS
select -(ROW_NUMBER() OVER(ORDER BY ACH_DATE desc)-1) as M_PAST,ACH_DATE from dates_filter01;   
-- e. select only last 2 month 
create TEMP TABLE measure_data AS
SELECT a.M_PAST, b.* 
from dates_filter as a LEFT JOIN measure_data01 as b on a.ACH_DATE = b.ACH_DATE; 

-- d. Create pivot data 
CREATE TEMP TABLE head as 
select DISTINCT M_PAST, ACH_DATE, ORG_CODE as GP_CD, IND_CODE as IND_CD from measure_data;
create TEMP TABLE num as 
select ACH_DATE, ORG_CODE as GP_CD, IND_CODE as IND_CD, sum(VALUE) as NUM from measure_data where MEASURE_TYPE = 'NUM'
group by ACH_DATE, ORG_CODE, IND_CODE;
create TEMP TABLE den as 
select  ACH_DATE, ORG_CODE as GP_CD, IND_CODE as IND_CD, sum(VALUE) as DEN from measure_data where MEASURE_TYPE = 'DEN' 
group by  ACH_DATE, ORG_CODE, IND_CODE;
create TEMP TABLE pca as 
select  ACH_DATE, ORG_CODE as GP_CD, IND_CODE as IND_CD, sum(VALUE) as PCA from measure_data where MEASURE_TYPE = 'PCA' 
group by  ACH_DATE, ORG_CODE, IND_CODE;
create temp TABLE pivot_data as
-- create TABLE pivot_data as
SELECT a.*, b.NUM, c.DEN, d.PCA 
from head as a 
left join num as b on a.ACH_DATE = b.ACH_DATE and a.GP_CD = b.GP_CD and a.IND_CD = b.IND_CD
left join den as c on a.ACH_DATE = c.ACH_DATE and a.GP_CD = c.GP_CD and a.IND_CD = c.IND_CD
left join pca as d on a.ACH_DATE = d.ACH_DATE and a.GP_CD = d.GP_CD and a.IND_CD = d.IND_CD;

-- 2. ADD ORGANISATION 
-- a. create relevant indicators 
CREATE TEMP TABLE indicators as 
SELECT a.IND_CD, b.PCG_CD 
from (SELECT DISTINCT IND_CD from pivot_data) as a left join scheme_info as b on a.IND_CD = b.NCD_CD;
CREATE TEMP TABLE org_ind AS
SELECT a.PCN_CD,a.PCN_NM,a.GP_CD,a.GP_NM, b.IND_CD, b.PCG_CD, c.M_PAST, c.ACH_DATE 
from organisation as a join indicators as b left join dates_filter as c;
-- b. add submissions
CREATE TEMP TABLE submissions AS
SELECT a.PCN_CD,a.PCN_NM,a.GP_CD,a.GP_NM, a.PCG_CD as IND_CD,a.M_PAST, a.ACH_DATE, b.NUM,b.DEN, b.PCA 
from org_ind as a LEFT JOIN pivot_data as b on a.GP_CD = b.GP_CD and a.IND_CD = b.IND_CD and a.ACH_DATE = b.ACH_DATE;

SELECT * from submissions limit 10;

-- 3. ADD ANALYSIS FLAGS
DROP TABLE if EXISTS fact_data_quality;
CREATE TABLE fact_data_quality as
select  *, 
case when NUM = 0 then "TRUE" else "FALSE" end as NUM_ZERO,
case when NUM is null then "TRUE" else "FALSE" end as NUM_NULL,
case when DEN = 0 then "TRUE" else "FALSE" end as DEN_ZERO,
case when DEN is null then "TRUE" else "FALSE" end as DEN_NULL
from submissions;

SELECT * from fact_data_quality limit 10;

-- 4. CREATE DIM SYSTEM SUPPLIER
DROP TABLE if EXISTS dim_system_suppliers;
CREATE TABLE dim_system_suppliers as 
select *
from sys_supp;