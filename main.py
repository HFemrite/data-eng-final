import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery

def run():
    total_views_new_table = bigquery.TableReference(
        projectId= "york-cdf-start",
        datasetId= "final_hunter_femrite",
        tableId= "cust_tier_code-sku-total_no_of_product_views"
    )
    total_sales_new_table = bigquery.TableReference(
        projectId= 'york-cdf-start',
        datasetId= 'final_hunter_femrite',
        tableId= 'cust_tier_code-sku-total_sales_amount'
    )
    views_schema = {
        'fields' : [
            {'name': 'cust_tier_code', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'sku', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'total_no_of_product_views', 'type': 'INTEGER', 'mode': 'REQUIRED'}
        ]
    }
    sales_schema = {
        'fields': [
            {'name': 'cust_tier_code', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'sku', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'total_sales_amount', 'type': 'FLOAT', 'mode': 'REQUIRED'}
        ]
    }
    o = beam.options.pipeline_options.PipelineOptions(
        project='york-cdf-start', region='us-central1', runner='DataflowRunner',
        temp_location='gs://hdf-york-proj-2/tmp', staging_location='gs://hdf-york-proj-2/staging',
        job_name='hunter-femrite-final-job'
    )
    with beam.Pipeline(options=o) as p:
        views_data = p | "Read_Views" >> beam.io.ReadFromBigQuery(query='SELECT cust_tier_code, sku, count(event_tm) as total_no_of_product_views FROM york-cdf-start.final_input_data.product_views as p JOIN york-cdf-start.final_input_data.customers as c ON p.customer_id = c.customer_id GROUP BY cust_tier_code, sku', use_standard_sql=True)
        views_data | "Write_Views" >> beam.io.WriteToBigQuery(table=total_views_new_table, schema=views_schema, create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
        sales_data = p | "Read_Sales" >> beam.io.ReadFromBigQuery(query='SELECT cust_tier_code, sku, round(sum(order_amt), 2) as total_sales_amount FROM york-cdf-start.final_input_data.orders as o JOIN york-cdf-start.final_input_data.customers as c ON o.customer_id = c.customer_id GROUP BY cust_tier_code, sku', use_standard_sql=True)
        sales_data | "Write_Sales" >> beam.io.WriteToBigQuery(table=total_sales_new_table, schema=sales_schema, create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

if __name__ == '__main__':
    run()
