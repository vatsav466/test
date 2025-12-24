import polars as pl
import json
import uuid
import xlsxwriter


def convert_to_polars(data):
    if isinstance(data, list):
        return pl.DataFrame(data, infer_schema_length=10000)

    if isinstance(data, str):
        return pl.DataFrame(json.loads(data), infer_schema_length=10000)

    return data




def VLookup(
        target_data,
        source_data,
        target_key_columns,
        source_key_columns,
        source_extra_columns=[],
        how='left',
        suffixes="_right",
        indicator=False,
        target_filter=None,
        source_filter=None,
        **kwargs
):
    """
    Merge dataframes based on left_on and right_on columns,
    with optional target/source filters. Remaining unfiltered
    target rows are appended back after merge.

    :param target_data: left dataframe
    :param source_data: right dataframe
    :param target_key_columns: left_on columns
    :param source_key_columns: right_on columns
    :param source_extra_columns: extra columns
    :param how: merge type
    :param suffixes: suffixes for merged columns
    :param indicator: whether to add indicator column
    :param target_filter: list of filter strings to apply on target_data
    :param source_filter: list of filter strings to apply on source_data
    :param kwargs: additional arguments

    :return: merged dataframe
    """
    if isinstance(target_data, list):
        target_data = convert_to_polars(target_data)
    if isinstance(source_data, list):
        source_data = convert_to_polars(source_data)

    print('target_columns --->', target_data.columns)
    print('source columns --->', source_data.columns)

    # --- Step 0: Add UUID column for remainder tracking ---
    target_data = target_data.with_columns(pl.Series("System_Idx", [str(uuid.uuid4()) for _ in range(target_data.height)]))

    # --- Step 1: Apply Target Filters ---
    filtered_target = target_data
    if target_filter:
        for f in target_filter:
            local_env = {"df": filtered_target, "pl": pl}
            exec(f, {}, local_env)
            filtered_target = local_env["df"]

        remainder_target = target_data.filter(~pl.col("System_Idx").is_in(filtered_target["System_Idx"].to_list()))
    else:
        remainder_target = target_data.clone()

    # --- Step 2: Apply Source Filters ---
    filtered_source = source_data
    if source_filter:
        for f in source_filter:
            local_env = {"df": filtered_source, "pl": pl}
            exec(f, {}, local_env)
            filtered_source = local_env["df"]

    # --- Step 3: Prepare columns for merge ---
    source_extra_columns = source_extra_columns if source_extra_columns else filtered_source.columns
    filtered_source = filtered_source.select(list(set(source_key_columns + source_extra_columns)))

    new_names = [f"{x}_tmp" for x in target_key_columns]

    filtered_target = filtered_target.with_columns(pl.col(x).alias(y) for x, y in zip(target_key_columns, new_names))
    filtered_source = filtered_source.with_columns(pl.col(x).alias(y) for x, y in zip(source_key_columns, new_names))

    filtered_target = filtered_target.with_columns(left_merge=pl.lit("Left"))
    filtered_source = filtered_source.with_columns(right_merge=pl.lit("Right"))

    # --- Step 4: Merge filtered parts ---
    merged_filtered = filtered_target.join(
        filtered_source, on=new_names, how=how, suffix=suffixes
    ).drop(new_names, strict=False)

    if indicator:
        merged_filtered = (
            merged_filtered
            .with_columns(
                _merge=pl.when((pl.col('left_merge').is_not_null()) & (pl.col("right_merge").is_null()))
                .then(pl.lit('left_only'))
                .when((pl.col('left_merge').is_null()) & (pl.col("right_merge").is_not_null()))
                .then(pl.lit('right_only'))
                .otherwise(pl.lit('both'))
                .alias('_merge')
            )
        )

    merged_filtered = merged_filtered.drop(["left_merge", "right_merge"], strict=False)

    _drop_cols = [x for x in merged_filtered.columns if x.endswith(suffixes)]
    if how == "left":
        merged_filtered = merged_filtered.drop(_drop_cols, strict=False)

    # --- Step 5: Append remainder target rows ---
    final_df = pl.concat([merged_filtered, remainder_target], how  = 'diagonal_relaxed')


    final_df = final_df.unique(subset = ["System_Idx"])

    # --- Step 6: Drop temp System_Idx ---
    final_df = final_df.drop("System_Idx", strict=False)

    return final_df


HSBC_BANK = pl.read_csv("/Users/macbook/Downloads/HSBC_DATA.csv",infer_schema_length=10000)

REFUND_ELIGBLE = pl.read_excel("/Users/macbook/Downloads/Postpaid Data/Query Data.xlsx")

QC_DATA = pl.read_excel("/Users/macbook/Downloads/Postpaid Data/Cliq cash Data.xlsx")

BANK_REJEECT = pl.read_excel("/Users/macbook/Downloads/Postpaid Data/BANK REJECTED on 11.12.2025.XLSX")

SAP = pl.read_excel("/Users/macbook/Downloads/Postpaid Data/F110 on 11.12.2025.XLSX")




REFUND_ELIGBLE = VLookup(target_data=REFUND_ELIGBLE, source_data=SAP.select(['Assignment','Document Number','Clearing Document']),
             target_key_columns=['transaction_id'], source_key_columns=['Assignment'],
             how = 'left', suffixes="_right", indicator=False, target_filter=[], source_filter=None)

REFUND_ELIGBLE = REFUND_ELIGBLE.rename({"Document Number":"KR DOCUMENT","Clearing Document":"ZP DOCUMENT"})


QC_DATA = QC_DATA.with_columns(pl.col('Transaction ID').alias('TRANSACTION_ID_NEW'))

REFUND_ELIGBLE = VLookup(target_data=REFUND_ELIGBLE, source_data=QC_DATA,
             target_key_columns=['transaction_id'], source_key_columns=['Transaction ID'],
             how = 'left', suffixes="_right", indicator=True, target_filter=[], source_filter=None,source_extra_columns=['TRANSACTION_ID_NEW'])


HSBC_BANK_SUCESS = HSBC_BANK.filter(pl.col('ResponseCode').is_in(['0','91','00']))
HSBC_BANK_FALIURE = HSBC_BANK.filter(~pl.col('ResponseCode').is_in(['0','91','00']))


print(HSBC_BANK_SUCESS['Transaction Date'].dtype)
print(HSBC_BANK_SUCESS['Transaction Date'].unique().to_list())

HSBC_BANK_SUCESS = HSBC_BANK_SUCESS.with_columns(pl.col("Payment Ref Number").str.slice(0, 10).alias("ZP No"))
HSBC_BANK_SUCESS = HSBC_BANK_SUCESS.with_columns(pl.col("Transaction Date").str.strptime(pl.Datetime, "%d/%m/%Y %H:%M:%S")
      .dt.strftime("%m-%d-%Y").alias("Refund Date"))


HSBC_BANK_FALIURE = HSBC_BANK_FALIURE.with_columns(pl.col("Payment Ref Number").str.slice(0, 10).alias("ZP No"))
HSBC_BANK_FALIURE = HSBC_BANK_FALIURE.with_columns(pl.col("Transaction Date")
      .str.strptime(pl.Datetime, "%d/%m/%Y %H:%M:%S").dt.strftime("%m-%d-%Y").alias("Refund Date"))

HSBC_BANK_SUCESS = HSBC_BANK_SUCESS.with_columns(pl.col('ZP No').alias('ZP_NO_NEW'))

REFUND_ELIGBLE = VLookup(target_data=REFUND_ELIGBLE, source_data=HSBC_BANK_SUCESS,
             target_key_columns=['ZP DOCUMENT'], source_key_columns=['ZP No'],
             how = 'left', suffixes="_right", indicator=True, target_filter=[], source_filter=None,source_extra_columns=['ZP_NO_NEW'])


REFUND_ELIGBLE = VLookup(target_data=REFUND_ELIGBLE, source_data=BANK_REJEECT.select(['KR Document','ZP Document']),
             target_key_columns=['KR DOCUMENT'], source_key_columns=['KR Document'],
             how = 'left', suffixes="_right", indicator=False, target_filter=[], source_filter=None)


REFUND_ELIGBLE = REFUND_ELIGBLE.rename({"ZP_NO_NEW":"BANK_SUCCESS","TRANSACTION_ID_NEW":"QC_REFUND","ZP Document":"BANK_REJECT"})


REFUND_ELIGBLE = VLookup(target_data=REFUND_ELIGBLE, source_data=HSBC_BANK_FALIURE.select(['ZP No','Description']),
             target_key_columns=['BANK_REJECT'], source_key_columns=['ZP No'],
             how = 'left', suffixes="_right", indicator=False, target_filter=[], source_filter=None)

REFUND_ELIGBLE = VLookup(target_data=REFUND_ELIGBLE, source_data=BANK_REJEECT.select(['ZP Document','Additional reason']),
             target_key_columns=['BANK_REJECT'], source_key_columns=['ZP Document'],
             how = 'left', suffixes="_right", indicator=False, target_filter=["df = df.filter(pl.col('Description').is_null())"], source_filter=None)
REFUND_ELIGBLE = REFUND_ELIGBLE.with_columns(
    (
        pl.col("Description").fill_null("") +
        pl.col("Additional reason").fill_null("")
    ).alias("BANK_REJECTED_REASON")
)





HSBC_BANK_SUCESS = HSBC_BANK_SUCESS.with_columns(pl.col("IMPS Reference No").cast(pl.Float64).cast(pl.Int64).cast(pl.Utf8))




REFUND_ELIGBLE = VLookup(target_data=REFUND_ELIGBLE, source_data=HSBC_BANK_SUCESS.select(['ZP No','IMPS Reference No']),
             target_key_columns=['BANK_SUCCESS'], source_key_columns=['ZP No'],
             how = 'left', suffixes="_right", indicator=False, target_filter=[], source_filter=None)

REFUND_ELIGBLE = REFUND_ELIGBLE.rename({"IMPS Reference No":"BANK_REF_NO"})

REFUND_ELIGBLE = VLookup(target_data=REFUND_ELIGBLE, source_data=HSBC_BANK_SUCESS.select(['ZP No','Refund Date']),
             target_key_columns=['BANK_SUCCESS'], source_key_columns=['ZP No'],
             how = 'left', suffixes="_right", indicator=False, target_filter=[], source_filter=None)

REFUND_ELIGBLE = VLookup(target_data=REFUND_ELIGBLE, source_data=BANK_REJEECT.select(['KR Document','ZP Document']),
             target_key_columns=['KR DOCUMENT'], source_key_columns=['KR Document'],
             how = 'left', suffixes="_right", indicator=False, target_filter=[], source_filter=None)

REFUND_ELIGBLE = REFUND_ELIGBLE.with_columns(pl.col('BANK_REF_NO').alias('BANK REFERENCE'))

REFUND_ELIGBLE = REFUND_ELIGBLE.rename({"ZP Document":"SAP_ZP_DOC","Refund Date":"REFUND_DATE"})




REFUND_ELIGBLE = REFUND_ELIGBLE.with_columns(pl.lit('').alias('REMARKS'))

REFUND_ELIGBLE = REFUND_ELIGBLE.with_columns(pl.when(pl.col("QC_REFUND").is_not_null() | (pl.col("QC_REFUND") == ""))
                 .then(pl.lit("Refund via EGV")).otherwise(pl.col("REMARKS")).alias("REMARKS"))

REFUND_ELIGBLE = REFUND_ELIGBLE.with_columns(pl.when(pl.col("BANK_SUCCESS").is_not_null() | (pl.col("BANK_SUCCESS") == ""))
                 .then(pl.lit("Refund via IMPS")).otherwise(pl.col("REMARKS")).alias("REMARKS"))

REFUND_ELIGBLE = REFUND_ELIGBLE.with_columns(pl.when((pl.col("REMARKS").is_null() | (pl.col("REMARKS") == "")) &
                 (pl.col("KR DOCUMENT").is_null() | (pl.col("KR DOCUMENT") == "")) & (pl.col("type") == "Postpaid CIR Replacement"))
                 .then(pl.lit("Replacement Order")).otherwise(pl.col("REMARKS")).alias("REMARKS"))

REFUND_ELIGBLE = REFUND_ELIGBLE.with_columns(pl.when((pl.col("REMARKS").is_null() | (pl.col("REMARKS") == "")) &
                 (pl.col("KR DOCUMENT").is_null() | (pl.col("KR DOCUMENT") == "")) & (pl.col("type") == "Postpaid CIR"))
                 .then(pl.lit("Auto KR Generated")).otherwise(pl.col("REMARKS")).alias("REMARKS"))

REFUND_ELIGBLE = REFUND_ELIGBLE.with_columns(pl.when((pl.col("REMARKS").is_null() | (pl.col("REMARKS") == "")) &
                 ~pl.col('BANK_REJECTED_REASON').is_in(['Timed Out', '']))
                .then(pl.lit('Bank Rejected')).otherwise(pl.col('REMARKS')).alias('REMARKS'))

REFUND_ELIGBLE = REFUND_ELIGBLE.with_columns(pl.when((pl.col("REMARKS").is_null() | (pl.col("REMARKS") == "")) &
                 pl.col('REMARKS').str.contains('Timed Out')).then(pl.lit('Refund via IMPS-Timed Out'))
                .otherwise(pl.col('REMARKS')).alias('REMARKS'))


REFUND_ELIGBLE = VLookup(target_data=REFUND_ELIGBLE, source_data=HSBC_BANK_SUCESS.select(['Beneficiary Name','Bene IFSC','ZP No']),
             target_key_columns=['ZP DOCUMENT'], source_key_columns=['ZP No'],
             how = 'left', suffixes="_right", indicator=False, target_filter=[], source_filter=None)



NEW_DATA = REFUND_ELIGBLE.with_columns(pl.lit('').alias('*REMARKS'))
NEW_DATA = NEW_DATA.with_columns(pl.when((pl.col("*REMARKS").is_null() | (pl.col('*REMARKS') == "")) &
           pl.col('REMARKS').str.contains('Auto KR Generated|Bank Rejected'))
           .then(pl.lit('Not Refunded')).otherwise(pl.col('*REMARKS')).alias('*REMARKS'))
NEW_DATA = NEW_DATA.with_columns(pl.when((pl.col("*REMARKS").is_null() | (pl.col('*REMARKS') == "")) &
           pl.col('REMARKS').str.contains('Refund via EGV'))
           .then(pl.lit('Refund via EGV')).otherwise(pl.col('*REMARKS')).alias('*REMARKS'))
NEW_DATA = NEW_DATA.with_columns(pl.when((pl.col("*REMARKS").is_null() | (pl.col('*REMARKS') == "")) &
           pl.col('REMARKS').str.contains('Refund via IMPS-Timed Out|Refund via IMPS'))
           .then(pl.lit('Refund via IMPS')).otherwise(pl.col('*REMARKS')).alias('*REMARKS'))
NEW_DATA = NEW_DATA.with_columns(pl.when((pl.col("*REMARKS").is_null() | (pl.col('*REMARKS') == "")) &
           pl.col('REMARKS').str.contains('Replacement Order'))
           .then(pl.lit('Replacement Order')).otherwise(pl.col('*REMARKS')).alias('*REMARKS'))




REFUND_ELIGBLE = REFUND_ELIGBLE[['type', 'transaction_id', 'date_time', 'order_reference_number', 'order_id', 'order_date', 'payment_mode', 'payment_mode_1', 'payment_type', 'payment_gateway', 'payment_reference_number', 'paid_amount',
                                 'shipping_charges', 'qc_paid_amount', 'qc_shipping_charges', 'shipping_type', 'cancel_date', 'packed_date', 'inscan_date', 'handed_over_to_courier_date', 'delivered_date', 'return_closed_date', 'refund_completed_date', 'qc_refund_amount',
                                 'total_refund_amount', 'bank_refund_amount', 'type_of_refund', 'qcr_transaction_date', 'qcr_card_program_group', 'qcr_invoice_number', 'qcr_transaction_id', 'qcr_amount', 'jr_gateway', 'jr_refund_arn', 'jr_refund_status', 'jr_refund_message',
                                 'jr_refund_date', 'jr_epg_txn_id', 'jr_refund_id', 'jr_refund_ref_id', 'jr_refund_amount', 'mode_of_refund','KR DOCUMENT','ZP DOCUMENT','QC_REFUND','BANK_SUCCESS','BANK_REJECT','BANK_REJECTED_REASON','BANK_REF_NO','REMARKS','BANK REFERENCE','REFUND_DATE','Beneficiary Name','Bene IFSC']]

HSBC_BANK_SUCESS = HSBC_BANK_SUCESS[['ZP No', 'Refund Date','IMPS Reference No', 'RequestId', 'Transaction Date', 'Transaction Type', 'Transaction Info', 'Remitter Mobile Number', 'Remitter MMID', 'Remitter Name', 'Beneficiary Name', 'Bene Merchant Mobile Number', 'Bene Merchant MMID',
                                     'Payment Ref Number', 'MCC', 'Amount', 'Debit Account Number', 'Credit Account Number', 'Status', 'MSP ID', 'ResponseCode', 'Description', 'Reversal Flag', 'Remitter Account Number', 'Beneficiary Account Number', 'Bene IFSC', 'Transaction Currency']]
HSBC_BANK_FALIURE = HSBC_BANK_FALIURE[[ 'ZP No', 'Refund Date','IMPS Reference No', 'RequestId', 'Transaction Date', 'Transaction Type', 'Transaction Info', 'Remitter Mobile Number', 'Remitter MMID', 'Remitter Name', 'Beneficiary Name', 'Bene Merchant Mobile Number', 'Bene Merchant MMID',
                                        'Payment Ref Number', 'MCC', 'Amount', 'Debit Account Number', 'Credit Account Number', 'Status', 'MSP ID', 'ResponseCode', 'Description', 'Reversal Flag', 'Remitter Account Number', 'Beneficiary Account Number', 'Bene IFSC', 'Transaction Currency']]

NEW_DATA = NEW_DATA[['date_time','KR DOCUMENT', 'order_reference_number','transaction_id','order_id','paid_amount','REFUND_DATE','REMARKS','ZP DOCUMENT','*REMARKS']]


path = '/Users/macbook/Downloads/POSTPAID_RECON_COD_23122025.xlsx'
with xlsxwriter.Workbook(path) as workbook:
        REFUND_ELIGBLE.write_excel(workbook=workbook, worksheet="REFUND_DATA",float_precision=2)
        HSBC_BANK_SUCESS.write_excel(workbook=workbook, worksheet="BANK_SUCESS",float_precision=2)
        HSBC_BANK_FALIURE.write_excel(workbook=workbook, worksheet="BANK_FALIURE",float_precision=2)
        BANK_REJEECT.write_excel(workbook=workbook, worksheet="BANK_REJECT_SAP_DATA",float_precision=2)
        NEW_DATA.write_excel(workbook=workbook, worksheet="VR_CONSOLE",float_precision=2,dtype_formats = {pl.Date : "dd-mm-yyyy"})
        # pivot_dff.write_excel(workbook=workbook, worksheet="SETTLEMENT2_PIVOT",float_precision=2)






