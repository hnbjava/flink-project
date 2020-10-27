package vip.hnbcao.elastic;


import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Copyright (C), 2019-2020, 中冶赛迪重庆信息技术有限公司
 * <p>
 * ClassName： ElasticsearchStreaming
 * <p>
 * Description：
 *
 * @author hnbcao
 * @version 1.0.0
 * @date 2020/10/23 上午9:06
 */

public class ElasticsearchStreaming {

    public static void main(String[] args) throws Exception {
        //1、设置运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(100);

        //2、配置数据源读取数据
        DataStream<String> sourceStream01 = env.readTextFile("data/input/wg_data.txt", "UTF-8");
        DataStream<String> sourceStream02 = env.readTextFile("data/input/wg_data_1.txt", "UTF-8");
        DataStream<String> sourceStream03 = env.readTextFile("data/input/wg_data_2.txt", "UTF-8");

        DataStream<String> sourceStream = sourceStream01.union(sourceStream02).union(sourceStream03);
        //3、处理数据
//        DataStream<DynamicSchemaData> dataStream = sourceStream.flatMap(new DynamicTranslator());
//
        //4、配置数据汇写出数据
        final SinkFunction<String> bulkSink = createElasticsearchSink();
//
//        sourceStream.addSink(bulkSink).name("sink");

        sourceStream.print();

        //5、提交执行
        env.execute("Streaming WordCount");
    }

    private static ElasticsearchSink<String> createElasticsearchSink() {
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("10.73.13.61", 9200, "http"));

        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(httpHosts, new ElasticsearchSinkFunction<String>() {
            @Override
            public void process(String element, RuntimeContext context, RequestIndexer indexer) {
                indexer.add(createIndexRequest(element));
            }

            private IndexRequest createIndexRequest(String element) {
                Map<String, String> json = new HashMap<>();
                json.put("data", element);
                return Requests.indexRequest()
                        .index("flink_test")
                        .source(json);
            }
        });

        esSinkBuilder.setBulkFlushMaxActions(1);

//        esSinkBuilder.setRestClientFactory(restClientBuilder -> {
//            restClientBuilder.setDefaultHeaders()
//        });
        return esSinkBuilder.build();
    }

}