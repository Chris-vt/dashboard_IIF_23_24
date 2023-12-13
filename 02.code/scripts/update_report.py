# %%
import sys,os
from prelude import *
from pathlib import Path
import pandas as pd
sys.path.append(os.getcwd())

from dslib import sqlib
from zipfile import ZipFile

os.system("cls")
print ("RUNNING REPORT UPDATE...")


bulkzip2223 = wd.inputs_dynamic.joinpath("Bulk_Ach_Ex_NCD2223_20220401_20230331.zip")

# >>>>>>>>>>>>>>>> update this target file to the latest data available <<<<<<<<<<<<<<<<<<<<<<<
bulkzip2324 = wd.inputs_dynamic.joinpath("Bulk_Ach_Ex_NCD2324_20230401_20231031.zip")
# >>>>>>>>>>>>>>>> update this target file to the latest data available <<<<<<<<<<<<<<<<<<<<<<<

def import_bulk(bulkzip)-> None:
    target_file  = wd.inputs_dynamic.joinpath(ZipFile(bulkzip).namelist()[0])
    with ZipFile(bulkzip, 'r') as zip_file:
        zip_file.extractall(wd.inputs_dynamic)
    sqlib.import_bulk_mod(file_name=target_file,tbl_name="bulk",db_name=db)
    sqlib.safe_remove(target_file)

def refresh_bulk2324(bulkzip)-> None:
    sqlib.execute_query_direct(f"DELETE FROM bulk where ACH_DATE >20230331",db=db)
    import_bulk(bulkzip=bulkzip)

def update_supporting_tables():
    epcn_zip    = wd.inputs_dynamic.joinpath("ePCN.zip")
    epcn_xls    = wd.inputs_dynamic.joinpath("ePCN.xlsx")
    epcn_docs   = wd.inputs_dynamic.joinpath("ePCN.xlsx Specification.pdf")
    clean_gps   = wd.inputs_dynamic.joinpath("clean_gps.csv")
    clean_pcns  = wd.inputs_dynamic.joinpath("clean_pcns.csv")
    gp_list     = wd.inputs_dynamic.joinpath("gp-reg-pat-prac-all.csv")
    clean_list  = wd.inputs_dynamic.joinpath("clean_list.csv")
    org_struct  = wd.inputs_static.joinpath("Output_Areas_(2021)_Sub_ICB_Locations_to_Integrated_Care_Boards_to_NHS_England_(Region)_(April_2023)_Lookup_in_England.csv")
    clean_org   = wd.inputs_dynamic.joinpath("clean_org.csv")
    measures    = wd.inputs_static.joinpath("in_measures.csv")
    scheme_info = wd.inputs_static.joinpath("in_scheme_info.csv")
    script      = wd.code_sqls.joinpath("01_update_supporting_tables.sql")
    sys_supp    = wd.inputs_static.joinpath("in_sys_supp.csv")
    acc08       = wd.inputs_static.joinpath("ACC-08_2023_05_24.csv")
    projections = wd.inputs_static.joinpath("in_projections.csv")

    #  clean up raw data inputs
    with ZipFile(epcn_zip, 'r') as zip_ref:
        zip_ref.extractall(wd.inputs_dynamic)
    gps =  pd.read_excel(epcn_xls,'PCN Core Partner Details')
    gps = gps.iloc[:,[0,1,4,8,9]].set_axis(['GP_CD','GP_NM','PCN_CD','StartRel','EndRel'],axis=1)
    gps.to_csv(clean_gps, index=False)
    pcns = pd.read_excel(epcn_xls,'PCNDetails',
                usecols=['PCN Code','PCN Name','Current Sub ICB Loc Code','Sub ICB Location','Open Date','Close Date'])
    pcns = pcns.iloc[:,[0,1,2,3,4,5]].set_axis(['PCN_CD','PCN_NM','SUBICB_CD','SUBICB_NM','Open','Close'],axis=1)
    pcns.to_csv(clean_pcns,index=False)
    list = pd.read_csv(gp_list)
    list = list.iloc[:,[1,5,7,8,9]].set_axis(['EXTRACT','GP_CD','SEX','AGE','PATS'],axis=1)
    list.to_csv(clean_list,index=False)
    org = pd.read_csv(org_struct)
    org = org.iloc[:,[2,3,5,6,8,9]].set_axis(['SUBICB_CD','SUBICB_NM','ICB_CD','ICB_NM','REG_CD','REG_NM'],axis=1)
    org.to_csv(clean_org,index=False)
    os.remove(epcn_xls)
    os.remove(epcn_docs)
    
    # Import in temporary database for processing
    sqlib.create_db(db)
    sqlib.import_from_csv(clean_gps,tableName='gps',db=db)
    sqlib.import_from_csv(clean_pcns,tableName='pcns',db=db)
    sqlib.import_from_csv(clean_list,tableName='pats',db=db)
    sqlib.import_from_csv(clean_org,tableName='org',db=db)
    os.remove(clean_gps)
    os.remove(clean_pcns)
    os.remove(clean_list)
    os.remove(clean_org)

    # Import static support tables measures and scheme info
    sqlib.execute_query_direct("DROP TABLE if EXISTS measures;",db=db)
    sqlib.execute_query_direct("DROP TABLE if EXISTS scheme_info;",db=db)
    sqlib.execute_query_direct("DROP TABLE if EXISTS sys_supp;",db=db)
    sqlib.import_from_csv(measures,tableName='measures',db=db)
    sqlib.import_from_csv(scheme_info,tableName='scheme_info',db=db)
    sqlib.import_from_csv(sys_supp,tableName='sys_supp',db=db)
    sqlib.import_from_csv(acc08,tableName='acc08',db=db)
    sqlib.import_from_csv(projections,tableName='projections',db=db)

    # execute query transformation
    sqlib.execute_query(script,db=db)

def build_iif_performance():
    script_data         = wd.code_sqls.joinpath("02_build_data.sql")
    script_achievement  = wd.code_sqls.joinpath("03_build_achievement.sql")
    script_payout       = wd.code_sqls.joinpath("04_build_payout.sql")
    script_dims         = wd.code_sqls.joinpath("05_build_dims.sql")
    fact_performance    = wd.outputs.joinpath("fact_performance.csv")
    fact_gpdata         = wd.outputs.joinpath("fact_gpdata.csv")
    dim_dates           = wd.outputs.joinpath("dim_dates.csv")
    dim_organisation    = wd.outputs.joinpath("dim_organisation.csv")
    dim_gpmapping       = wd.outputs.joinpath("dim_gpmapping.csv")
    dim_scheme_info     = wd.outputs.joinpath("dim_scheme_info.csv")
    sqlib.execute_query(script_data,db=db)
    sqlib.execute_query(script_achievement,db=db)
    sqlib.execute_query(script_payout,db=db)
    sqlib.execute_query(script_dims,db=db)
    sqlib.safe_remove(fact_performance)
    sqlib.safe_remove(dim_dates)
    sqlib.safe_remove(dim_organisation)
    sqlib.safe_remove(dim_scheme_info)
    sqlib.export_to_csv(db,'fact_performance',fact_performance)
    sqlib.export_to_csv(db,'fact_gpdata',fact_gpdata)
    sqlib.export_to_csv(db,'dim_dates',dim_dates)
    sqlib.export_to_csv(db,'dim_organisation',dim_organisation)
    sqlib.export_to_csv(db,'dim_gpmapping',dim_gpmapping)
    sqlib.export_to_csv(db,'dim_scheme_info',dim_scheme_info)

def build_data_quality():
    script              = wd.code_sqls.joinpath("06_quality_build.sql")
    fact_data_quality   = wd.outputs.joinpath("fact_data_quality.csv")
    dim_system_suppliers= wd.outputs.joinpath("dim_system_suppliers.csv") 
    sqlib.execute_query(script,db=db)
    sqlib.safe_remove(fact_data_quality)
    sqlib.safe_remove(dim_system_suppliers)
    sqlib.export_to_csv(db,'fact_data_quality',fact_data_quality)
    sqlib.export_to_csv(db,'dim_system_suppliers',dim_system_suppliers)

# routine execution
import time
from numpy import round

# t2 = time.time()
# print(f"1. Importing data 23/24")
# refresh_bulk2324(bulkzip=bulkzip2324)
# print(f"[completed in {round((time.time() - t2),1)} seconds]")
t3 = time.time()
print(f"2. Build supporting tables")
update_supporting_tables()
print(f"[completed in {round((time.time() - t3),1)} seconds]")
t4 = time.time()
print(f"3. Build iif performance")
build_iif_performance()
print(f"[completed in {round((time.time() -t4),1)} seconds]")
t5 = time.time()
print(f"4. Build data quality")
build_data_quality()
print(f"[completed in {round((time.time() -t5),1)} seconds]")