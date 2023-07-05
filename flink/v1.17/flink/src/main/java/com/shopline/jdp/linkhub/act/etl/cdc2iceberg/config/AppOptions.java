package com.shopline.jdp.linkhub.act.etl.cdc2iceberg.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * App Options.
 *
 * @Author: eric
 * @Date: 2023/2/14 16:21
 * @Description:
 */
public interface AppOptions {

  ConfigOption<String> SOURCE_KAFKA_BROKERS =
      ConfigOptions.key("source.kafka.brokers").stringType().noDefaultValue();
  ConfigOption<String> SOURCE_KAFKA_TOPIC_REGEX =
      ConfigOptions.key("source.kafka.topic-regex").stringType().noDefaultValue();
  ConfigOption<String> FILL_TOPIC_NAME =
      ConfigOptions.key("source.kafka.fill-topic").stringType().noDefaultValue();
  ConfigOption<Boolean> SOURCE_KAFKA_SASL_ENABLED =
      ConfigOptions.key("source.kafka.sasl.enabled").booleanType().defaultValue(false);
  ConfigOption<String> SOURCE_KAFKA_SASL_USER =
      ConfigOptions.key("source.kafka.sasl.user").stringType().noDefaultValue();
  ConfigOption<String> SOURCE_KAFKA_SASL_PASSWORD =
      ConfigOptions.key("source.kafka.sasl.password").stringType().noDefaultValue();
  ConfigOption<String> SOURCE_KAFKA_GROUP_ID =
      ConfigOptions.key("source.consumer.group-id").stringType().noDefaultValue();
  ConfigOption<String> SOURCE_ADAPTIVE_SCHEMA_KAFKA_INFO =
      ConfigOptions.key("source.adaptive-schema.kafka-info").stringType().noDefaultValue();
  ConfigOption<String> SINK_ICEBERG_IDENTIFIER =
      ConfigOptions.key("sink.iceberg.table-identifier").stringType().noDefaultValue();
  ConfigOption<String> HDFS_HIVE_SITE =
      ConfigOptions.key("hdfs.hive-site").stringType().defaultValue("hive-conf-dir");
  ConfigOption<String> HDFS_KEYTAB =
      ConfigOptions.key("hdfs.keytab").stringType().defaultValue("dist-keytab");
  ConfigOption<Boolean> REFRESH_TABLE_PROP_ENABLED =
      ConfigOptions.key("sink.refresh.table-properties.enabled").booleanType().defaultValue(true);
  ConfigOption<String> STP_UPDATE_TIME_COL =
      ConfigOptions.key("update_time.column-name").stringType().defaultValue("update_time");
  ConfigOption<Boolean> CDC_IGNORE_UPDATE_BEFORE =
      ConfigOptions.key("cdc.ignore.update_before").booleanType().defaultValue(true);
  ConfigOption<String> CDC_LATEST_TIME_PROPERTY =
      ConfigOptions.key("cdc.update_time.latest.properties").stringType()
          .defaultValue("stp.latest.update-time");
  ConfigOption<String> SER_CDC_TIME_FORMAT =
      ConfigOptions.key("serialize.cdc.time.format").stringType()
          .defaultValue("TIMESTAMP");
  ConfigOption<String> SER_PARTITION_BASE_COL =
      ConfigOptions.key("serialize.partition.base.column-name").stringType().noDefaultValue();
  ConfigOption<String> SER_PARTITION_COL_DT =
      ConfigOptions.key("serialize.partition-dt.column-name").stringType().defaultValue("dt");
  ConfigOption<String> SER_PARTITION_COL_HR =
      ConfigOptions.key("serialize.partition-hr.column-name").stringType().defaultValue("hr");
  ConfigOption<String> LOGIC_DATASOURCE_CONF =
      ConfigOptions.key("logic_datasource.conf").stringType().noDefaultValue();
  ConfigOption<String> SPARK_URL =
      ConfigOptions.key("spark-client.url").stringType().noDefaultValue();

  ConfigOption<String> LOGIC_SCHEMA_QUERY_URL =
      ConfigOptions.key("logic.schema-query.url")
          .stringType()
          .noDefaultValue()
          .withDescription("dbms url");
  ConfigOption<String> SOURCE_BIZ_GROUP =
      ConfigOptions.key("source.biz.group")
          .stringType()
          .noDefaultValue()
          .withDescription("任务的业务分组");


  ConfigOption<Boolean> TEST_ENABLED =
      ConfigOptions.key("test.enabled")
          .booleanType()
          .defaultValue(false)
          .withDescription("是否开启测试模式");


  ConfigOption<String> ETL_JOB_TYPE =
      ConfigOptions.key("job.type")
          .stringType()
          .noDefaultValue()
          .withDescription("入湖作业类型, 流批一体/一键集成");


  ConfigOption<Long> SOURCE_KAFKA_CLUSTER_ID =
      ConfigOptions.key("source.kafka.cluster-id")
          .longType()
          .noDefaultValue()
          .withDescription("source端kafka cluster id");


  ConfigOption<String> SPARK_CLIENT =
      ConfigOptions.key("spark-client.url")
          .stringType()
          .noDefaultValue()
          .withDescription("keytab path");
}
