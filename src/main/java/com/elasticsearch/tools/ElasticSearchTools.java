package com.elasticsearch.tools;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class ElasticSearchTools {

	
	public static RestHighLevelClient client = null;
    public final static String HOST = "127.0.0.1"; //服务器部署
    public final static Integer PORT = 9200; //端口
    
    public static RestHighLevelClient getClient(){
    	client = new RestHighLevelClient(RestClient.builder(new HttpHost(new HttpHost(HOST, PORT, "http"))));
        return client;
    }
    
    public static RestHighLevelClient getClient2(){
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("127.0.0.1", 9200, "http"),
                        new HttpHost("127.0.0.1", 9201, "http"),
                        new HttpHost("127.0.0.1", 9202, "http")));

        return client;
    }
    
    public static void closeClient(RestHighLevelClient client){
    	if(client != null){
    		try{
    			client.close();
    		}catch(IOException e){
    			e.printStackTrace();
    		}
    	}
    }
    
    
    /**
     * 检查索引是否存在
     * @param indexName
     * @return
     * @throws IOException
     */
    public static boolean checkExistIndex(String indexName, RestHighLevelClient client) throws IOException {
        boolean result =true;
        try {
            OpenIndexRequest openIndexRequest = new OpenIndexRequest(indexName);
            client.indices().open(openIndexRequest).isAcknowledged();
        } catch (ElasticsearchStatusException ex) {
            String m = "Elasticsearch exception [type=index_not_found_exception, reason=no such index]";
            if (m.equals(ex.getMessage())) {
                result = false;
            }
        }
        if(result)
            System.out.println("索引:" +indexName + " 是存在的");
        else
            System.out.println("索引:" +indexName + " 不存在");
         
        return result;
    }
    
}
