package com.elasticsearch.service;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.elasticsearch.bean.UserInfo;
import com.elasticsearch.tools.ElasticSearchTools;
import com.elasticsearch.tools.SourceUtils;

public class BatchService {

	private static final Logger log = LoggerFactory.getLogger(SearchService.class);
    private static RestHighLevelClient restHighLevelClient = ElasticSearchTools.getClient2();
    
    public static void main(String[] args) throws IOException {
    	String indexName = "test-user";
    	List<UserInfo> userList = SourceUtils.file2list("users.txt");
        System.out.println("准备数据，总计"+userList.size()+"条");
        
        batchInsert(userList, indexName);

        ElasticSearchTools.closeClient(restHighLevelClient);
    }
    
    
    public static void batchInsert(List<UserInfo> users, String indexName) throws IOException{
    	BulkRequest bulkRequest = new BulkRequest();
    	for(UserInfo userInfo : users){
    		Map<String, Object> map = userInfo.toMap();
    		IndexRequest indexRequest= new IndexRequest(indexName, "doc", String.valueOf(new Random().nextInt()*1000000)).source(map);
    		bulkRequest.add(indexRequest);
    	}
    	
    	restHighLevelClient.bulk(bulkRequest);
        System.out.println("批量插入完成");
    }
    
    
    /**
     * 增加文档信息
     */
    public static void addDocument(String indexName) {
        try {
            IndexRequest indexRequest = new IndexRequest(indexName, "doc", "1");
            UserInfo userInfo = new UserInfo();
            userInfo.setName("张三");
            userInfo.setAge(29);
            userInfo.setSalary(100.00f);
            userInfo.setAddress("北京市");
            userInfo.setRemark("来自北京市的张先生");
            userInfo.setCreateTime(new Date());
            userInfo.setBirthDate("1990-01-10");

            byte[] json = JSON.toJSONBytes(userInfo);
            indexRequest.source(json, XContentType.JSON);
            IndexResponse response = restHighLevelClient.index(indexRequest);
            log.info("创建状态：{}", response.status());
        } catch (Exception e) {
            log.error("", e);
        }
    }
}
