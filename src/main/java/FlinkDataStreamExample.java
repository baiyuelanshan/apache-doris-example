import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;
import org.apache.doris.flink.source.DorisSource;
import org.apache.doris.flink.source.DorisSourceBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Properties;

public class FlinkDataStreamExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().inStreamingMode().build();

        /*
        RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend("file:///project/rw/apache-doris-example/src/main/statebackend",true);
        rocksDBStateBackend.setNumberOfTransferThreads(1);
        rocksDBStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
        env.setStateBackend((StateBackend)rocksDBStateBackend);
         */

        env.enableCheckpointing(10);
        env.setParallelism(1);


        //Doris Source
        DorisOptions.Builder sourceBuilder =
                DorisOptions.builder()
                        .setFenodes("192.168.56.104:8030")  //FE节点IP和端口
                        .setTableIdentifier("test.order_info_example")
                        .setUsername("test")
                        .setPassword("password123");


        DorisSource<List<?>> dorisSource = DorisSourceBuilder.<List<?>>builder()
                .setDorisOptions(sourceBuilder.build())
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDeserializer(new SimpleListDeserializationSchema())
                .build();

        //Doris Sink
        DorisSink.Builder<String> sinkBuilder = DorisSink.builder();
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder.setFenodes("192.168.56.104:8030")
                .setTableIdentifier("test.order_info_output")
                .setUsername("test")
                .setPassword("password123");

        Properties properties = new Properties();
        properties.setProperty("column_separator", ",");
        DorisExecutionOptions.Builder  executionBuilder = DorisExecutionOptions.builder();
        executionBuilder.setLabelPrefix("label-doris-20") //streamload label prefix
                .setStreamLoadProp(properties);
        sinkBuilder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setDorisOptions(dorisBuilder.build())
                .setSerializer(new SimpleStringSerializer()) //serialize according to string
                 ;



        DataStreamSource<List<?>> source = env.fromSource(dorisSource, WatermarkStrategy.noWatermarks(), "doris source");
        DataStream<String> transform = source.flatMap(new FlatMapFunction<List<?>, String>() {
            @Override
            public void flatMap(List<?> element, Collector<String> collector) throws Exception {

                //collector.collect();
                StringBuffer stringBuffer = new StringBuffer();
                stringBuffer.append(element.get(0))
                        .append(",")
                        .append(element.get(1))
                        .append(",")
                        .append(element.get(2))
                        .append(",")
                        .append(element.get(3))
                        .append(",")
                        .append(element.get(4))
                        .append(",")
                        .append(element.get(5))
                        ;
                    collector.collect(stringBuffer.toString());

            }
        });


        transform.print();
        transform.sinkTo(sinkBuilder.build());


        env.execute("Flink DataStream example");


    }
}
