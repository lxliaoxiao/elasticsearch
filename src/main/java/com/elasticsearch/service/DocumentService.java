package com.elasticsearch.service;

import com.alibaba.fastjson.JSON;
import com.elasticsearch.bean.UserInfo;
import com.elasticsearch.tools.ElasticSearchTools;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;


public class DocumentService {

	private static final Logger log = LoggerFactory.getLogger(DocumentService.class);
    private static RestHighLevelClient restHighLevelClient = ElasticSearchTools.getClient2();
    
    public static void main(String[] args) throws IOException {
    	String indexName = "test-user";
         
        if(ElasticSearchTools.checkExistIndex(indexName, restHighLevelClient)){
//        	addDocument(indexName);
        	getDocument(indexName);
        	
//        	updateDocument(indexName);
//        	deleteDocument(indexName);
        }

        ElasticSearchTools.closeClient(restHighLevelClient);
    }
    
    /**
     * 增加文档信息
     */
    public static void addDocument(String indexName) {
        try {
            UserInfo userInfo = new UserInfo();
            userInfo.setName("张三");
            userInfo.setAge(29);
            userInfo.setSalary(100.00f);
            userInfo.setAddress("北京市");
            userInfo.setRemark("来自北京市的张先生");
            userInfo.setCreateTime(new Date());
            userInfo.setBirthDate("1990-01-10");

            IndexRequest indexRequest = new IndexRequest(indexName, "doc", "1");
            indexRequest.source(JSON.toJSONBytes(userInfo), XContentType.JSON);
            IndexResponse response = restHighLevelClient.index(indexRequest);
        } catch (Exception e) {
            log.error("", e);
        }
    }

    /**
     * 获取文档信息
     */
    public static void getDocument(String indexName) {
        try {
            GetRequest getRequest = new GetRequest(indexName, "doc", "1");
            GetResponse getResponse = restHighLevelClient.get(getRequest);
            if (getResponse.isExists()) {
                UserInfo userInfo = JSON.parseObject(getResponse.getSourceAsBytes(), UserInfo.class);
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }

    /**
     * 更新文档信息
     */
    public static void updateDocument(String indexName) {
        try {
            UpdateRequest updateRequest = new UpdateRequest(indexName, "doc", "1");
            UserInfo userInfo = new UserInfo();
            userInfo.setSalary(200f);
            userInfo.setAddress("深圳市南山区");
            userInfo.setAge(28);

            byte[] json = JSON.toJSONBytes(userInfo);
            updateRequest.doc(json, XContentType.JSON);
            UpdateResponse response = restHighLevelClient.update(updateRequest);
            log.info("创建状态：{}", response.status());
        } catch (Exception e) {
            log.error("", e);
        }
    }

    /**
     * 删除文档信息
     */
    public static void deleteDocument(String indexName) {
        try {
            DeleteRequest deleteRequest = new DeleteRequest(indexName, "doc", "1");
            DeleteResponse response = restHighLevelClient.delete(deleteRequest);
            log.info("删除状态：{}", response.status());
        } catch (IOException e) {
            log.error("", e);
        }
    }

}
