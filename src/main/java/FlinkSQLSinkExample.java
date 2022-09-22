import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSQLSinkExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        //开启checkpoint, check interval为1000毫秒
        env.enableCheckpointing(1000);
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, environmentSettings);
        /* 使用datagen生成测试数据
        'rows-per-second' =  '10' 每秒发送10条数据
        'number-of-rows' = '100'  一共发送100条数据，不设置的话会无限量发送数据
         */
        tenv.executeSql("CREATE TABLE order_info_source (\n" +
                "    order_date DATE,\n" +
                "    order_id     INT,\n" +
                "    buy_num      INT,\n" +
                "    user_id      INT,\n" +
                "    create_time  TIMESTAMP(3),\n" +
                "    update_time   TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' =  '10',\n" +
                "  'fields.order_id.min' = '30001',\n" +
                "  'fields.order_id.max' = '30500',\n" +
                "  'fields.user_id.min' = '10001',\n" +
                "  'fields.user_id.max' = '20001',\n" +
                "  'fields.buy_num.min' = '10',\n" +
                "  'fields.buy_num.max' = '20',\n" +
                "  'number-of-rows' = '100'" +
                ")");


        /* 用于查看datagen生成的数据
        tenv.executeSql("CREATE TABLE print_table (\n" +
                "    order_date DATE,\n" +
                "    order_id     INT,\n" +
                "    buy_num      INT,\n" +
                "    user_id      INT,\n" +
                "    create_time  TIMESTAMP(3),\n" +
                "    update_time   TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")");
        tenv.executeSql("insert into order_info_sink select * from order_info_source");

         */

        //注册Doris Sink表
        tenv.executeSql("CREATE TABLE order_info_sink (  \n" +
                "order_date DATE,  \n" +
                "order_id INT,  \n" +
                "buy_num INT,\n" +
                "user_id INT,\n" +
                "create_time TIMESTAMP(3),\n" +
                "update_time TIMESTAMP(3)\n" +
                ")  \n" +
                "WITH (\n" +
                "'connector' = 'doris',   \n" +
                "'fenodes' = '192.168.56.104:8030',   \n" +
                "'table.identifier' = 'test.order_info_example',   \n" +
                "'username' = 'test',   \n" +
                "'password' = 'password123',   \n" +
                "'sink.label-prefix' = 'sink_doris_label_8'\n" +
                        ")"
                );


        tenv.executeSql("insert into order_info_sink select * from order_info_source");


    }
}
