
CREATE EXTERNAL TABLE 'customer'(
  'customername` string COMMENT 'from deserializer', 
  'email' string COMMENT 'from deserializer', 
  'phone' string COMMENT 'from deserializer', 
  'Birthday' string COMMENT 'from deserializer', 
  'serialnumber' string COMMENT 'from deserializer', 
  'registrationdate' bigint COMMENT 'from deserializer', 
  'lastupdatedate' bigint COMMENT 'from deserializer', 
  'sharewithresearchasofdate' bigint COMMENT 'from deserializer',  
  'sharewithfriendsasofdate' bigint COMMENT 'from deserializer')
  
PARTITIONED BY ( 
  'partition_0' string)
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES ( 
  'paths'='birthDay,customerName,email,lastUpdateDate,phone,registrationDate,serialNumber,shareWithFriendsAsOfDate,shareWithPublicAsOfDate,shareWithResearchAsOfDate') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'

LOCATION
  's3://myproject-files/customer/'
  
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='Customer_LandingZone', 
  'averageRecordSize'='307', 
  'classification'='json', 
  'compressionType'='none', 
  'objectCount'='1',
  'sizeKey'='3298200', 
  'typeOfData'='file')
