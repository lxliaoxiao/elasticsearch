package com.elasticsearch.service;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.elasticsearch.tools.ElasticSearchTools;

import java.io.IOException;


public class IndexService {

	private static final Logger log = LoggerFactory.getLogger(IndexService.class);
//    private static RestHighLevelClient restHighLevelClient = ElasticSearchTools.getClient();
    
    private static RestHighLevelClient restHighLevelClient = ElasticSearchTools.getClient2();
    
    
    public static void main(String[] args) throws IOException {
        String indexName = "test-user";
         
        if(!ElasticSearchTools.checkExistIndex(indexName, restHighLevelClient)){
            createIndex(indexName);        
        }
        
//        if(ElasticSearchTools.checkExistIndex(indexName, restHighLevelClient)){
//        	deleteIndex(indexName);
//        }

        ElasticSearchTools.checkExistIndex(indexName, restHighLevelClient);
        ElasticSearchTools.closeClient(restHighLevelClient);
        
    }
    
    
    /**
     * 创建索引
     */
    public static void createIndex(String indexName) {
        try {
            XContentBuilder mapping = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("dynamic", true)
                    .startObject("properties")

                    .startObject("name")
                    .field("type", "text")
                    .startObject("fields")
                    .startObject("keyword")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject()
                    
                    .startObject("address")
                    .field("type", "text")
                    .startObject("fields")
                    .startObject("keyword")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject()
                    
                    .startObject("remark")
                    .field("type", "text")
                    .startObject("fields")
                    .startObject("keyword")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject()
                    
                    .startObject("age")
                    .field("type", "integer")
                    .endObject()
                    
                    .startObject("salary")
                    .field("type", "float")
                    .endObject()
                    
                    .startObject("birthDate")
                    .field("type", "date")
                    .field("format", "yyyy-MM-dd")
                    .endObject()
                    
                    .startObject("createTime")
                    .field("type", "date")
                    .endObject()

                    .endObject()
                    .endObject();

            //创建索引配置信息
            Settings settings = Settings.builder()
                    .put("index.number_of_shards", 2) //分片数量
                    .put("index.number_of_replicas", 2)  //副本
                    .build();

            //创建索引请求对象，然后设置索引类型和 mapping
            CreateIndexRequest request = new CreateIndexRequest(indexName, settings);
            request.mapping("doc", mapping);

            //创建索引
            CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(request);
            boolean isCreated = createIndexResponse.isAcknowledged();
            log.info("是否创建成功：{}", isCreated);
        } catch (IOException e) {
        	e.printStackTrace();
            log.error("", e);
        }
    }


    /**
     * 删除索引
     */
    public static void deleteIndex(String indexName) {
        try {
            // 新建删除索引请求对象
            DeleteIndexRequest request = new DeleteIndexRequest(indexName);
            AcknowledgedResponse acknowledgedResponse = restHighLevelClient.indices().delete(request);
            boolean siDeleted = acknowledgedResponse.isAcknowledged();
            log.info("是否删除成功：{}", siDeleted);
        } catch (IOException e) {
        	e.printStackTrace();
            log.error("", e);
        }
    }

}
