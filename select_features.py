import sys
from pyspark import SparkContext

if __name__ == "__main__":
    file = sys.argv[1] #raw train file
    sc = SparkContext(appName="CTR_Features")
    data = sc.textFile(file).map(lambda line: line.encode("utf8", "ignore").split(','))
    #count feature
    device_id_click = data.map(lambda fields: (fields[11],int(fields[1]))).reduceByKey(lambda v1,v2: v1+v2)
    device_id_impression = data.map(lambda fields: (fields[11],1)).reduceByKey(lambda v1,v2: v1+v2)

    device_ip_click = data.map(lambda fields: (fields[12],int(fields[1]))).reduceByKey(lambda v1,v2: v1+v2)
    device_ip_impression = data.map(lambda fields: (fields[12],1)).reduceByKey(lambda v1,v2: v1+v2)

    site_app_category_click = data.map(lambda fields: (fields[7] + "_" + fields[10],int(fields[1]))).reduceByKey(lambda v1,v2: v1+v2)
    site_app_category_impression = data.map(lambda fields: (fields[7] + "_" + fields[10],1)).reduceByKey(lambda v1,v2: v1+v2)

    ad_id_click = data.map(lambda fields: (fields[0],fields[1])).reduceByKey(lambda v1,v2: v1+v2)
    ad_id_impression = data.map(lambda fields: (fields[0],1)).reduceByKey(lambda v1,v2: v1+v2)

    ad_app_id_click = data.map(lambda fields: (fields[0] + "_" + fields[8],int(fields[1]))).reduceByKey(lambda v1,v2: v1+v2)
    ad_app_id_impression = data.map(lambda fields: (fields[0] + "_" + fields[8],1)).reduceByKey(lambda v1,v2: v1+v2)

    device_id_click.saveAsTextFile("device_id_click")
    device_id_impression.saveAsTextFile("device_id_impression")

    device_ip_click.saveAsTextFile("device_ip_click")
    device_ip_impression.saveAsTextFile("device_ip_impression")

    site_app_category_click.saveAsTextFile("site_app_category_click")
    site_app_category_impression.saveAsTextFile("site_app_category_impression")

    ad_id_click.saveAsTextFile("ad_id_click")
    ad_id_impression.saveAsTextFile("ad_id_impression")

    ad_app_id_click.saveAsTextFile("ad_app_id_click")
    ad_app_id_impression.saveAsTextFile("ad_app_id_impression")
