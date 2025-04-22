create external table if not exists 'stedi-project'.'customer_landing'(
'serialnumber' string,
'sharewithpubliccaseofdate;' bigint,
'birthday' string,
'registerdate' bigint,
'customername' string
'email'  string
'lastdatedate' bigint
'phone' string
'sharewithfriendofdate' bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.jsonjsonserde'
WITH SERDEPROPERTIES(
'ignore.malformed.json'='FALSE',
'dots.in.keys'='TRUE',
'mapping'='TRUE'
)
STORED AS INPUTFORMAT'org.apache.hadoop.mapred.TextInputFormat'outputformat
'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION's3://stedi-lake-houseusername123/accelerometer-1691348231445.json'
TBLPROPERTIES('classification'='json')
