-- 5. CREATE OUT DATE DERIVED DIMENSION
-- a) create dates
CREATE temp TABLE dates01 as
select distinct ACH_DATE 
from fact_performance order by ACH_DATE desc;
DROP TABLE IF EXISTS dim_dates; 
CREATE TABLE dim_dates as
select ACH_DATE, 
CASE
when ACH_DATE<=20230331 then '22/23'
else '23/24' end as FY,
cast(SUBSTR(ACH_DATE,0,7) as integer) as YM,
SUBSTR(ACH_DATE,0,5) ||'_' || SUBSTR(ACH_DATE,5,2) as Y_M,
-(ROW_NUMBER() OVER(ORDER BY ACH_DATE desc)-1) as M_PAST,
case 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 4 then 1 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 5 then 2 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 6 then 3 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 7 then 4 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 8 then 5 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 9 then 6 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 10 then 7 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 11 then 8 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 12 then 9 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 1 then 10 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 2 then 11 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 3 then 12 
end as M_FY,
case 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 4 then 'APR' 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 5 then 'MAY' 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 6 then 'JUN' 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 7 then 'JUL'
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 8 then 'AUG' 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 9 then 'SEP' 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 10 then 'OCT' 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 11 then 'NOV' 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 12 then 'DEC' 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 1 then 'JAN' 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 2 then 'FEB' 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 3 then 'MAR' 
end as M_NM
from dates01 order by ACH_DATE desc;

-- 6. CREATE OUT ORGANISATION DIMENSION
DROP TABLE if EXISTS dim_organisation;
CREATE TABLE dim_organisation AS
SELECT distinct LAST_UPDATE,REG_CD,REG_NM,ICB_CD,ICB_NM,SUBICB_CD,SUBICB_NM,PCN_CD,PCN_NM
from organisation;

DROP TABLE if EXISTS dim_gpmapping;
CREATE TABLE dim_gpmapping AS
SELECT distinct LAST_UPDATE,REG_CD,REG_NM,ICB_CD,ICB_NM,SUBICB_CD,SUBICB_NM,PCN_CD,PCN_NM, GP_CD, GP_NM
from organisation;

-- 7. CREATE OUT SCHEME INFO DIMENSION
DROP TABLE IF EXISTS dim_scheme_info;
CREATE TABLE dim_scheme_info AS
SELECT PCG_CD as IND_CD, NCD_CD,IND_AREA,IND_DESC,IND_TYPE,IND_LT as LT, IND_UT as UT, PTS, PT_VAL, BUDGET_2324 
from scheme_info;