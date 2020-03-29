import pandas as pd
import numpy as np
import pyodbc
import math
import logging
import os
from azure.storage.blob import BlockBlobService
from dfply import *
from datetime import date
from azure.common.credentials import ServicePrincipalCredentials
from azure.keyvault import KeyVaultAuthentication, KeyVaultClient
from flask import Flask
app = Flask(__name__)

@app.route("/")

def __init__():
    logging.basicConfig(format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    
    logger.info('START: Geting credentials')
    
    app_id = 'e744dc6d-3d62-4879-8f43-e2ca01a49720'
    tenant_id = '8e9a02ca-d0ea-4763-900c-ac6ee988b360'
    app_secret = 'Y38uym/cZuA]S4.W/Cnnzc9P0645dn4M'
    credentials = ServicePrincipalCredentials(client_id=app_id, secret=app_secret, tenant=tenant_id)
    secret_client = KeyVaultClient(credentials)
    vault_base_url = "https://kv-dse-scccth-prod01.vault.azure.net/"
    
    secret_name = "pctbag-db-host"
    secure_secrets_versions = secret_client.get_secret_versions(vault_base_url, secret_name)
    current_secret_version = sorted(list(secure_secrets_versions), key=lambda x: x.attributes.created, reverse=True)[0]
    current_secret_version_id = current_secret_version.id[-32:] 
    secure_secret = secret_client.get_secret(vault_base_url, secret_name, current_secret_version_id)
    db_host = secure_secret.value
    #print("db_host = " + str(db_host))
    
    secret_name = "pctbag-db-name"
    secure_secrets_versions = secret_client.get_secret_versions(vault_base_url, secret_name)
    current_secret_version = sorted(list(secure_secrets_versions), key=lambda x: x.attributes.created, reverse=True)[0]
    current_secret_version_id = current_secret_version.id[-32:] 
    secure_secret = secret_client.get_secret(vault_base_url, secret_name, current_secret_version_id)
    db_name = secure_secret.value
    #print("db_name = " + str(db_name))
    
    secret_name = "pctbag-db-username"
    secure_secrets_versions = secret_client.get_secret_versions(vault_base_url, secret_name)
    current_secret_version = sorted(list(secure_secrets_versions), key=lambda x: x.attributes.created, reverse=True)[0]
    current_secret_version_id = current_secret_version.id[-32:] 
    secure_secret = secret_client.get_secret(vault_base_url, secret_name, current_secret_version_id)
    db_user = secure_secret.value
    #print("db_username = " + str(db_user))
    
    secret_name = "pctbag-db-password"
    secure_secrets_versions = secret_client.get_secret_versions(vault_base_url, secret_name)
    current_secret_version = sorted(list(secure_secrets_versions), key=lambda x: x.attributes.created, reverse=True)[0]
    current_secret_version_id = current_secret_version.id[-32:] 
    secure_secret = secret_client.get_secret(vault_base_url, secret_name, current_secret_version_id)
    db_password = secure_secret.value
    
    logger.info('START: Establish database connection')
    driver_name = '{ODBC Driver 17 for SQL Server}'
    connection_string = 'DRIVER='+ driver_name + ';SERVER=' + db_host + ';PORT=1433;DATABASE=' + db_name + ';UID=' + db_user + ';PWD=' + db_password
    connection  = pyodbc.connect(connection_string)
    logger.info('START: Query DB Table - pctbag_tlkpMonthToNum')
    monthmap = pd.read_sql("SELECT * FROM pctbag_tlkpMonthToNum", connection) 
    logger.info('START: Query DB Table - pctbag_tlkpSKUMapping')
    SKU_map = pd.read_sql("SELECT * FROM pctbag_tlkpSKUMapping", connection)
    logger.info('START: Query DB Table - pctbag_tlkpSegmentMapping')
    seg_map = pd.read_sql("SELECT * FROM pctbag_tlkpSegmentMapping", connection)
    logger.info('START: Query DB Table - pctbag_tlkpBagProductCost')
    mrg_rp = pd.read_sql("SELECT * FROM pctbag_tlkpBagProductCost", connection)
    logger.info('START: Query DB Table - pctbag_tlkpLogParameters')
    log_para = pd.read_sql("SELECT * FROM pctbag_tlkpLogParameters", connection)
    logger.info('START: Query DB Table - pctbag_tlkpAreaProvinceMapping')
    areaMapping = pd.read_sql("SELECT * FROM pctbag_tlkpAreaProvinceMapping", connection)
    logger.info('START: Query DB Table - pctbag_tlkBagSalesRep')
    salesRep = pd.read_sql("SELECT * FROM pctbag_tlkBagSalesRep", connection)
    logger.info('START: Query DB Table - SAP BI')
    data = pd.read_sql("SELECT * FROM dbo.pct_bag_sapbi", connection)
    
    trans_as_of_date = data['Calendar Day'].max()
    updated_date = date.today()

    bkp_data = data
    bkp_monthmap = monthmap
    bkp_SKU_map = SKU_map
    bkp_seg_map = seg_map
    bkp_mrg_rp = mrg_rp
    bkp_log_para = log_para
    bkp_areaMapping = areaMapping
    bkp_salesRep = salesRep
    bkp_trans_as_of_date = trans_as_of_date
    bkp_updated_date = updated_date
    
    logger.info('START: Generating Power BI Data - PGT')
    
    data = pd.merge(data,
                    monthmap,
                    how='inner',
                    left_on=['Calendar Year', 'Calendar month'],
                    right_on=['CalendarYear', 'CalendarMonth'])
    
    CM = data['MonthNo'].max()
    LM = CM - 1
    
    df = data \
            >> mask(X['MonthNo'] >= LM) \
            >> select(
                    X['Sold-to Area (SCCC)'], \
                    X['Sold-to Province'], \
                    X['MonthNo'], \
                    X['Product Hierarchy Level 4'], \
                    X['Volume Sold'], \
                    X['Contribution Margin'] \
                     ) \
            >> mutate(\
                     Volume_last_month = (X['MonthNo']==LM) * X['Volume Sold'], \
                     Volume_current_month = (X['MonthNo']==CM) * X['Volume Sold'], \
                     ) \
            >> group_by(\
                      X['Sold-to Area (SCCC)'], \
                      X['Sold-to Province'], \
                      X['Product Hierarchy Level 4'], \
                      ) \
            >> summarize(\
                    Contribution_Margin_last_month = (X['Contribution Margin'] * X['Volume_last_month']).sum()/X['Volume_last_month'].sum(),\
                    Contribution_Margin_current_month = (X['Contribution Margin'] * X['Volume_current_month']).sum()/ X['Volume_current_month'].sum(),\
                    Volume_last_month = X['Volume_last_month'].sum(),\
                    Volume_current_month = X['Volume_current_month'].sum(),\
                        )
    
    SKU_map['Product Hierarchy Level 4'] = SKU_map['SKU']
    data_final = pd.merge(df, 
                          SKU_map, 
                          how='left', 
                          on='Product Hierarchy Level 4')
    
    data_final = data_final[[
        'Sold-to Area (SCCC)',
        'Sold-to Province',
        'Product Hierarchy Level 4',
        'SKU1',
        'BagWeight',
        'Contribution_Margin_last_month',
        'Contribution_Margin_current_month',
        'Volume_last_month',
        'Volume_current_month'
    ]]
    
    data_final[['Contribution_Margin_last_month']] = data_final[['Contribution_Margin_last_month']].replace([np.inf, -np.inf], np.nan)
    data_final[['Contribution_Margin_current_month']] = data_final[['Contribution_Margin_current_month']].replace([np.inf, -np.inf], np.nan)
    data_final[['Volume_last_month']] = data_final[['Volume_last_month']].replace([np.inf, -np.inf], np.nan)
    data_final[['Volume_last_month']] = data_final[['Volume_current_month']].replace([np.inf, -np.inf], np.nan)
    
    
    data_final.rename({'Sold-to Area (SCCC)': 'Area', 'Sold-to Province': 'SoldToProvince', 'Product Hierarchy Level 4':'ProductHierarchyLevel4', 'Contribution_Margin_last_month':'ContributionMarginLM', 'Contribution_Margin_current_month':'ContributionMarginCM', 'Volume_last_month':'VolumeLM', 'Volume_current_month':'VolumeCM'}, axis=1, inplace=True)
    
    data_final['DataAsOfDate'] = trans_as_of_date
    data_final['UpdatedDate'] = updated_date
    
    pgt_all = data_final
    
    logger.info('END: Generating Power BI data (Price Gap Tracker) /' + str(len(pgt_all)) + ' records')
    
    logger.info('START: Writing output csv file - PGT')
    
    secret_name = "pctbag-pgt-ofilename"
    secure_secrets_versions = secret_client.get_secret_versions(vault_base_url, secret_name)
    current_secret_version = sorted(list(secure_secrets_versions), key=lambda x: x.attributes.created, reverse=True)[0]
    current_secret_version_id = current_secret_version.id[-32:] 
    secure_secret = secret_client.get_secret(vault_base_url, secret_name, current_secret_version_id)
    pgt_ofilename = secure_secret.value
    pgt_all.to_csv(pgt_ofilename, index=False)
    
    logger.info('START: Moving result to blob storage - PGT')
    
    secret_name = "pctbag-st-name"
    secure_secrets_versions = secret_client.get_secret_versions(vault_base_url, secret_name)
    current_secret_version = sorted(list(secure_secrets_versions), key=lambda x: x.attributes.created, reverse=True)[0]
    current_secret_version_id = current_secret_version.id[-32:] 
    secure_secret = secret_client.get_secret(vault_base_url, secret_name, current_secret_version_id)
    account_name = secure_secret.value
    
    secret_name = "pctbag-st-accesskey"
    secure_secrets_versions = secret_client.get_secret_versions(vault_base_url, secret_name)
    current_secret_version = sorted(list(secure_secrets_versions), key=lambda x: x.attributes.created, reverse=True)[0]
    current_secret_version_id = current_secret_version.id[-32:] 
    secure_secret = secret_client.get_secret(vault_base_url, secret_name, current_secret_version_id)
    account_key = secure_secret.value
    
    secret_name = "pctbag-st-pbiContainers"
    secure_secrets_versions = secret_client.get_secret_versions(vault_base_url, secret_name)
    current_secret_version = sorted(list(secure_secrets_versions), key=lambda x: x.attributes.created, reverse=True)[0]
    current_secret_version_id = current_secret_version.id[-32:] 
    secure_secret = secret_client.get_secret(vault_base_url, secret_name, current_secret_version_id)
    container_name = secure_secret.value
    
    block_blob_service = BlockBlobService(account_name, account_key)
    
    with open(pgt_ofilename, 'r') as myfile:
         file_content = myfile.read()
    
    block_blob_service.create_blob_from_text(container_name, pgt_ofilename, file_content)
    
    logger.info('END: Write result to blob storage - PGT')

    logger.info('START: RESET CONFIGURATION - TAB6n7')    
    sap_data = bkp_data
    month_map = bkp_monthmap 
    seg_map = bkp_seg_map
    mrg_rp = bkp_mrg_rp
    log_para = bkp_log_para
    trans_as_of_date = bkp_trans_as_of_date
    updated_date = bkp_updated_date
    logger.info('END: RESET CONFIGURATION - TAB6n7')
    
    logger.info('START: Generating Power BI Data (Tab6)')
    
    # Map month number to SAP data
    sap_data['Calendar year'] = sap_data['Calendar Year']
    sap_data['Quantity of Sale Unit'] = sap_data['Volume Sold']
    
    sap_data1 = pd.merge(sap_data,
                        month_map,
                        how = 'left',
                        left_on = ['Calendar year','Calendar month'],
                        right_on = ['CalendarYear','CalendarMonth'])
    
    sap_data1 = sap_data1.drop(['CalendarYear','CalendarMonth'], axis=1)

    max_month = sap_data1['MonthNo'].max()
    min_month = sap_data1['MonthNo'].min()
    
    rolling_8months = sap_data1 \
                            >> mask((X['Order - Price Category'] == "Maintain Market") | (X['Order - Price Category']=="Normal Sales")) \
                            >> mask((X['Sold-to Sales Office'] == "NSD-1") | (X['Sold-to Sales Office'] == "NSD-2")) \
                            >> mask(X['MonthNo'] > max_month -8 ) \
                            >> group_by(X['Sold-to Customer']) \
                            >> summarize( Volume = X['Quantity of Sale Unit'].sum())
    
    # Map segments 
    sap_data2 = pd.merge(sap_data1,
                         seg_map,
                         how = 'inner',
                         left_on=['Sold-to Area (SCCC)', 'Product Hierarchy Level 4'],
                         right_on=['Area', 'ProductHierarchyLevel4'])
    
    sap_data2 = sap_data2.drop(['Area','ProductHierarchyLevel4'], axis=1)

    # Map var costs and packing costs from mrgin report 
    
    sap_data2 = pd.merge(sap_data2,
                         mrg_rp,
                         how = 'inner',
                         left_on=['Calendar month', 'Calendar year', 'Product Hierarchy Level 4'],
                         right_on=['CalendarMonth', 'CalendarYear', 'ProductHierarchyLevel4'])
    
    sap_data2 = sap_data2.drop(['CalendarYear','CalendarMonth', 'ProductHierarchyLevel4'], axis=1)

    ## Collate data for segment customer tab
    
    custCM = sap_data2 \
                >> mask((X['Order - Price Category'] == "Maintain Market") | (X['Order - Price Category']=="Normal Sales")) \
                >> mask((X['Sold-to Sales Office'] == "NSD-1") | (X['Sold-to Sales Office'] == "NSD-2")) \
                >> mask(X['MonthNo'] > max_month - 8 ) \
                >> mutate(\
                         Total_Discount_rebate = X['Discount'] + X['Rebate'], \
                         Contribution_Margin = X['Marketing Net Sales'] - X['VariableProductionCost']- X['PackingCost'] \
                         )\
                >> group_by(\
                           X['Segment'], \
                           X['Sold-to Customer']\
                           )\
                >> summarize(\
                            Segment_Volume = X['Quantity of Sale Unit'].sum(),\
                            CM = (X['Quantity of Sale Unit']*X['Contribution_Margin']).sum()\
                            ) \
                >> mutate(Actual_CM = X['CM']/X['Segment_Volume']) \
                >> select(\
                         X['Segment'], \
                         X['Sold-to Customer'], \
                         X['Actual_CM'])\

    # Join CM with 8 months volume
    curveData = pd.merge(custCM,
                         rolling_8months,
                         how = 'inner',
                         on = 'Sold-to Customer')

    # Join curveData with segment wise log curve parameters 

    curveData["Segment"] = pd.to_numeric(curveData["Segment"])
    log_para["Segment"] = pd.to_numeric(log_para["Segment"])

    curveData = pd.merge(curveData,
                         log_para,
                         how = 'inner',
                         left_on=['Segment'],
                         right_on=['Segment'])

    curveData['Target_CM'] = curveData['Intercept'] + (curveData['Coefficient'] * (np.log(curveData['Volume'])/np.log(curveData['LogBase'])))
    curveData['Yellow_line'] = curveData['Target_CM'] + curveData['YellowDelta']
    curveData['Black_line'] = curveData['BlackLine']
    curveData['Current_Volume'] = curveData['Volume']
    
    curveData = curveData[['Segment','Sold-to Customer','Current_Volume','Actual_CM','Target_CM','Black_line','Yellow_line']]
    
    curveData.rename({'Sold-to Customer': 'SoldToCustomer', 'Current_Volume': 'CurrentVolume', 'Actual_CM':'ActualContributionMargin', 'Target_CM':'TargetContributionMargin', 'Black_line':'BlackLine', 'Yellow_line':'YellowLine'}, axis=1, inplace=True)
    curveData['DataAsOfDate'] = trans_as_of_date
    curveData['UpdatedDate'] = updated_date
    
    logger.info('END: Generating Power BI data (Tab6 Curve Data) /' + str(len(curveData)) + ' records')
    
    logger.info('START: Writing output csv file (Tab6 Curve Data)')
    
    secret_name = "pctbag-tab6-ofilename"
    secure_secrets_versions = secret_client.get_secret_versions(vault_base_url, secret_name)
    current_secret_version = sorted(list(secure_secrets_versions), key=lambda x: x.attributes.created, reverse=True)[0]
    current_secret_version_id = current_secret_version.id[-32:] 
    secure_secret = secret_client.get_secret(vault_base_url, secret_name, current_secret_version_id)
    tab6_ofilename = secure_secret.value
    
    curveData.to_csv(tab6_ofilename, index=False)
    
    logger.info('START: Moving result to blob storage (Tab6 Curve Data)')
    
    secret_name = "pctbag-st-name"
    secure_secrets_versions = secret_client.get_secret_versions(vault_base_url, secret_name)
    current_secret_version = sorted(list(secure_secrets_versions), key=lambda x: x.attributes.created, reverse=True)[0]
    current_secret_version_id = current_secret_version.id[-32:] 
    secure_secret = secret_client.get_secret(vault_base_url, secret_name, current_secret_version_id)
    account_name = secure_secret.value
    
    secret_name = "pctbag-st-accesskey"
    secure_secrets_versions = secret_client.get_secret_versions(vault_base_url, secret_name)
    current_secret_version = sorted(list(secure_secrets_versions), key=lambda x: x.attributes.created, reverse=True)[0]
    current_secret_version_id = current_secret_version.id[-32:] 
    secure_secret = secret_client.get_secret(vault_base_url, secret_name, current_secret_version_id)
    account_key = secure_secret.value
    
    secret_name = "pctbag-st-pbiContainers"
    secure_secrets_versions = secret_client.get_secret_versions(vault_base_url, secret_name)
    current_secret_version = sorted(list(secure_secrets_versions), key=lambda x: x.attributes.created, reverse=True)[0]
    current_secret_version_id = current_secret_version.id[-32:] 
    secure_secret = secret_client.get_secret(vault_base_url, secret_name, current_secret_version_id)
    container_name = secure_secret.value
    
    block_blob_service = BlockBlobService(account_name, account_key)
    
    with open(tab6_ofilename, 'r') as myfile:
         file_content = myfile.read()
    
    block_blob_service.create_blob_from_text(container_name, tab6_ofilename, file_content)
    
    logger.info('END: Write result to blob storage (Tab6 Curve Data)')
    
    ## 2. Data for current month and past month performance
    
    max_month = sap_data1['MonthNo'].max()
    
    rolling_8months_vol = sap_data1 \
                            >> mask((X['Order - Price Category'] == "Maintain Market") | (X['Order - Price Category']=="Normal Sales")) \
                            >> mask((X['Sold-to Sales Office'] == "NSD-1") | (X['Sold-to Sales Office'] == "NSD-2")) \
                            >> mutate(\
                                      Volume_baseline = ((X['MonthNo'] > (max_month-8)) & (X['MonthNo'] <= (max_month-1))) * X['Quantity of Sale Unit'],\
                                      Volume_current = ((X['MonthNo'] > (max_month-8)) & (X['MonthNo'] <= (max_month))) * X['Quantity of Sale Unit']\
                                     )\
                            >> group_by(X['Sold-to Customer']) \
                            >> summarize(\
                                         Volume_baseline = X['Volume_baseline'].sum(),\
                                         Volume_current = X['Volume_current'].sum()\
                                        )
    
    segmentData = sap_data2 \
                    >> mask((X['Order - Price Category'] == "Maintain Market") | (X['Order - Price Category']=="Normal Sales")) \
                    >> mask((X['Sold-to Sales Office'] == "NSD-1") | (X['Sold-to Sales Office'] == "NSD-2")) \
                    >> mutate(\
                             cm = X['Marketing Net Sales'] - X['VariableProductionCost'] - X['PackingCost'],\
                             discount = X['Discount'] + X['Rebate'],
                             vol_baseline = ((X['MonthNo'] > (max_month-8)) & (X['MonthNo'] <= (max_month-1))) * X['Quantity of Sale Unit'],\
                             vol_current = ((X['MonthNo'] > (max_month-8)) & (X['MonthNo'] <= (max_month))) * X['Quantity of Sale Unit'],\
                             vol_currentMonth =  (X['MonthNo'] == (max_month)) * X['Quantity of Sale Unit'],\
                             vol_lastMonth =  (X['MonthNo'] == (max_month-1)) * X['Quantity of Sale Unit']\
                               )\
                    >> mutate(\
                              cm_baseline = ((X['MonthNo'] > (max_month-8)) & (X['MonthNo'] <= (max_month-1))) * X['cm'] * X['Quantity of Sale Unit'],\
                              cm_current = ((X['MonthNo'] > (max_month-8)) & (X['MonthNo'] <= (max_month))) * X['cm'] * X['Quantity of Sale Unit'],\
                              cm_currentMonth = (X['MonthNo'] == (max_month)) * X['cm'] * X['Quantity of Sale Unit'],\
                              cm_lastMonth = (X['MonthNo'] == (max_month-1)) * X['cm'] * X['Quantity of Sale Unit'],\
                              discount_currentMonth = (X['MonthNo'] == (max_month)) * X['discount'] * X['Quantity of Sale Unit'],\
                              discount_lastMonth = (X['MonthNo'] == (max_month-1)) * X['discount'] * X['Quantity of Sale Unit']\
                             )\
                    >> group_by(\
                               X['Segment'],\
                               X['Sold-to Customer'] \
                               )\
                    >> summarize(\
                                Volume_baseline = X['vol_baseline'].sum(),\
                                Volume_current = X['vol_current'].sum(),\
                                discount_currentMonth = X['discount_currentMonth'].sum(),\
                                discount_lastMonth = X['discount_lastMonth'].sum(),\
                                cm_currentMonth = X['cm_currentMonth'].sum(),\
                                cm_lastMonth = X['cm_lastMonth'].sum(),\
                                Vol_currentMonth = X['vol_currentMonth'].sum(),\
                                Vol_lastMonth = X['vol_lastMonth'].sum(),\
                                cm_baseline = X['cm_baseline'].sum(),\
                                cm_current = X['cm_current'].sum()\
                                )\
                    >> mutate(\
                                cm_baseline = (X['cm_baseline']/X['Volume_baseline']),\
                                cm_current = (X['cm_current']/X['Volume_current'])\
                                )\
                    >> select(\
                             X['Segment'],\
                             X['Sold-to Customer'],\
                             X['cm_baseline'],\
                             X['cm_current'],\
                             X['cm_currentMonth'],\
                             X['cm_lastMonth'],\
                             X['Vol_currentMonth'],\
                             X['Vol_lastMonth'],\
                             X['discount_currentMonth'],\
                             X['discount_lastMonth']\
                             )

    logger.info('DONE: POINT 3')

    segmentData = pd.merge(segmentData,
                           rolling_8months_vol,
                           how = 'inner',
                           on = "Sold-to Customer")

    segmentData["Segment"] = pd.to_numeric(segmentData["Segment"])
    log_para["Segment"] = pd.to_numeric(log_para["Segment"])

    segmentData = pd.merge(segmentData,
                           log_para,
                           how = 'inner',
                           left_on=['Segment'],
                           right_on=['Segment'])
    
    segmentData['Target_CM_baseline'] = segmentData['Intercept'] + (segmentData['Coefficient'] * (np.log(segmentData['Volume_baseline'])/np.log(segmentData['LogBase'])))
    segmentData['Target_CM_current'] = segmentData['Intercept'] + (segmentData['Coefficient'] * (np.log(segmentData['Volume_current'])/np.log(segmentData['LogBase'])))
    segmentData['Yellow_line_baseline'] = segmentData['Target_CM_baseline']  + segmentData['YellowDelta']
    segmentData['Yellow_line_current'] = segmentData['Target_CM_current']  + segmentData['YellowDelta']
    segmentData['Black_line'] = segmentData['BlackLine']
    
    segmentData['Scenario_baseline'] = np.where(
        segmentData['cm_baseline'] >= segmentData['Target_CM_baseline'], 'A',
        np.where(
            segmentData['cm_baseline'] >= segmentData['Yellow_line_baseline'], 'B',
            np.where(
                segmentData['cm_baseline'] >= segmentData['Black_line'],
                'C', 'D')))
    
    segmentData['Scenario_current'] = np.where(
        segmentData['cm_current'] >= segmentData['Target_CM_current'], 'A',
        np.where(
            segmentData['cm_current'] >= segmentData['Yellow_line_current'], 'B',
            np.where(
                segmentData['cm_current'] >= segmentData['Black_line'],
                'C', 'D')))

    segmentData = segmentData[['Segment',
                 'Scenario_baseline',
                 'Scenario_current',
                 'cm_currentMonth',
                 'cm_lastMonth',
                 'Vol_currentMonth',
                 'Vol_lastMonth',
                 'discount_currentMonth',
                 'discount_lastMonth']]
    
    segmentData.rename({'Scenario_baseline': 'ScenarioBaseline', 'Scenario_current': 'ScenarioCurrent', 'cm_currentMonth':'ContributionMarginCM', 'cm_lastMonth':'ContributionMarginLM', 'Contribution_Margin_current_month':'ContributionMarginCM', 'Vol_lastMonth':'VolumeLM', 'Vol_currentMonth':'VolumeCM', 'discount_currentMonth':'DiscountCM', 'discount_lastMonth':'DiscountLM'}, axis=1, inplace=True)
    segmentData['DataAsOfDate'] = trans_as_of_date
    segmentData['UpdatedDate'] = updated_date
    
    logger.info('END: Generating Power BI data (Tab6 & 7) /' + str(len(segmentData)) + ' records')
    
    logger.info('START: Writing output csv file (Tab6 & 7)')
    
    secret_name = "pctbag-t67-ofilename"
    secure_secrets_versions = secret_client.get_secret_versions(vault_base_url, secret_name)
    current_secret_version = sorted(list(secure_secrets_versions), key=lambda x: x.attributes.created, reverse=True)[0]
    current_secret_version_id = current_secret_version.id[-32:] 
    secure_secret = secret_client.get_secret(vault_base_url, secret_name, current_secret_version_id)
    t67_ofilename = secure_secret.value
    segmentData.to_csv(t67_ofilename, index=False)
    
    logger.info('START: Moving result to blob storage (Tab6 & 7)')
    
    secret_name = "pctbag-st-name"
    secure_secrets_versions = secret_client.get_secret_versions(vault_base_url, secret_name)
    current_secret_version = sorted(list(secure_secrets_versions), key=lambda x: x.attributes.created, reverse=True)[0]
    current_secret_version_id = current_secret_version.id[-32:] 
    secure_secret = secret_client.get_secret(vault_base_url, secret_name, current_secret_version_id)
    account_name = secure_secret.value
    
    secret_name = "pctbag-st-accesskey"
    secure_secrets_versions = secret_client.get_secret_versions(vault_base_url, secret_name)
    current_secret_version = sorted(list(secure_secrets_versions), key=lambda x: x.attributes.created, reverse=True)[0]
    current_secret_version_id = current_secret_version.id[-32:] 
    secure_secret = secret_client.get_secret(vault_base_url, secret_name, current_secret_version_id)
    account_key = secure_secret.value
    
    secret_name = "pctbag-st-pbiContainers"
    secure_secrets_versions = secret_client.get_secret_versions(vault_base_url, secret_name)
    current_secret_version = sorted(list(secure_secrets_versions), key=lambda x: x.attributes.created, reverse=True)[0]
    current_secret_version_id = current_secret_version.id[-32:] 
    secure_secret = secret_client.get_secret(vault_base_url, secret_name, current_secret_version_id)
    container_name = secure_secret.value
    
    block_blob_service = BlockBlobService(account_name, account_key)
    
    with open(t67_ofilename, 'r') as myfile:
         file_content = myfile.read()
    
    block_blob_service.create_blob_from_text(container_name, t67_ofilename, file_content)
    
    logger.info('END: Write result to blob storage (Tab6 & 7)')

    connection.close()
    return 'SUCCESS'





