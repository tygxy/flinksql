package com.cmbc.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

import java.sql.Timestamp;

import static org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE;


public class SQLStreaming {
    public static void main(String[] args) throws Exception {

        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // kafka source
        Kafka kafka = new Kafka()
                .version("0.10")
                .topic("flink-streaming")
                .property("bootstrap.servers", "localhost:9092")
                .property("zookeeper.connect", "localhost:2181");
        tableEnv.connect(kafka)
                .withFormat(
                        new Json().failOnMissingField(true).deriveSchema()
                )
                .withSchema(
                        new Schema()
                                .field("id", Types.INT)
                                .field("site", Types.STRING)
                                .field("proctime", Types.SQL_TIMESTAMP).proctime()
                )
                .inAppendMode()
                .registerTableSource("Data");

        // do sql
        String sql = "SELECT TUMBLE_END(proctime, INTERVAL '5' SECOND) as processtime," +
                "count(1) as pv, site " +
                "FROM Data " +
                "GROUP BY TUMBLE(proctime, INTERVAL '5' SECOND), site";
        Table table = tableEnv.sqlQuery(sql);

        // to sink
        DataStream<Info> result = tableEnv.toAppendStream(table, Info.class);
        result.print();
        tableEnv.execute("Flink SQL in Streaming");
    }

    public static class Info {
        public Timestamp processtime;
        public String site;
        public Long pv;

        public Info() {
        }

        public Info(Timestamp processtime, String site, Long pv) {
            this.processtime = processtime;
            this.pv = pv;
            this.site = site;
        }

        @Override
        public String toString() {
            return
                    "processtime=" + processtime +
                            ", site=" + site +
                            ", pv=" + pv +
                            "";
        }
    }
}
