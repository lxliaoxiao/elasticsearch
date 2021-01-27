package com.elasticsearch.service;

import java.io.IOException;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.elasticsearch.bean.UserInfo;
import com.elasticsearch.tools.ElasticSearchTools;

public class SearchService {
	
	private static final Logger log = LoggerFactory.getLogger(SearchService.class);
    private static RestHighLevelClient restHighLevelClient = ElasticSearchTools.getClient2();
    
    public static void main(String[] args) throws IOException{
    	String indexName = "test-user";
    	//精确查询
//    	termQuery(indexName);
//    	termsQuery(indexName);
    	
    	//匹配查询
//    	matchAllQuery(indexName);
    	matchQuery(indexName);
//    	matchPhraseQuery(indexName);
//    	matchMultiQuery(indexName);
    	
    	//模糊查询
//    	fuzzyQuery(indexName);
    	
    	//范围查询
    	rangeQuery(indexName);
    	
    	//通配符查询
//    	wildcardQuery(indexName);
    	
    	// Bool查询
//    	boolQuery(indexName);
    	
    	ElasticSearchTools.closeClient(restHighLevelClient);
    }
    
    /**
     * 精确查询（查询条件不会进行分词，但是查询内容可能会分词，导致查询不到）
     */
    public static void termQuery(String indexName) {
        try {
            // 构建查询条件（注意：termQuery 支持多种格式查询，如 boolean、int、double、string 等，这里使用的是 string 的查询）
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.termQuery("address.keyword", "北京市"));
            
            // 创建查询请求对象，将查询对象配置到其中
            SearchRequest searchRequest = new SearchRequest(indexName);
            searchRequest.source(searchSourceBuilder);
            
            // 执行查询，根据状态和数据条数验证是否返回了数据
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
            if (RestStatus.OK.equals(searchResponse.status()) && searchResponse.getHits().totalHits > 0) {
                SearchHits hits = searchResponse.getHits();
                for (SearchHit hit : hits) {
                    UserInfo userInfo = JSON.parseObject(hit.getSourceAsString(), UserInfo.class);
                    log.info(userInfo.toString());
                }
            }
            
        } catch (IOException e) {
            log.error("", e);
        }
    }
 
    /**
     * 多个内容在一个字段中进行查询
     */
    public static void termsQuery(String indexName) {
        try {
            // 构建查询条件（注意：termsQuery 支持多种格式查询，如 boolean、int、double、string 等，这里使用的是 string 的查询）
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.termsQuery("address.keyword", "北京市丰台区", "北京市昌平区", "北京市大兴区"));
            
            // 创建查询请求对象，将查询对象配置到其中
            SearchRequest searchRequest = new SearchRequest(indexName);
            searchRequest.source(searchSourceBuilder);
            
            // 执行查询，根据状态和数据条数验证是否返回了数据
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
            if (RestStatus.OK.equals(searchResponse.status()) && searchResponse.getHits().totalHits > 0) {
                SearchHits hits = searchResponse.getHits();
                for (SearchHit hit : hits) {
                    UserInfo userInfo = JSON.parseObject(hit.getSourceAsString(), UserInfo.class);
                    log.info(userInfo.toString());
                }
            }
            
        } catch (IOException e) {
            log.error("", e);
        }
    }
    
    

    /**
     * 匹配查询符合条件的所有数据，并设置分页
     */
    public static void matchAllQuery(String indexName) {
        try {
            MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery(); //构建查询条件
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(matchAllQueryBuilder); 
            
            // 设置分页
            searchSourceBuilder.from(0);
            searchSourceBuilder.size(30);
            // 设置排序
            searchSourceBuilder.sort("salary", SortOrder.ASC);
            
            // 创建查询请求对象，将查询对象配置到其中
            SearchRequest searchRequest = new SearchRequest(indexName);
            searchRequest.source(searchSourceBuilder);
            
            // 执行查询，根据状态和数据条数验证是否返回了数据
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
            if (RestStatus.OK.equals(searchResponse.status()) && searchResponse.getHits().totalHits > 0) {
                SearchHits hits = searchResponse.getHits();
                for (SearchHit hit : hits) {
                    UserInfo userInfo = JSON.parseObject(hit.getSourceAsString(), UserInfo.class);
                    log.info(userInfo.toString());
                }
                log.info("---------------------------------------------------------------");
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }
    
    

    /**
     * 匹配查询数据
     */
    public static void matchQuery(String indexName) {
        try {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchQuery("address", "南山区"));  //构建查询条件
            
            // 创建查询请求对象，将查询对象配置到其中
            SearchRequest searchRequest = new SearchRequest(indexName);
            searchRequest.source(searchSourceBuilder);
            
            // 执行查询，根据状态和数据条数验证是否返回了数据
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
            if (RestStatus.OK.equals(searchResponse.status()) && searchResponse.getHits().totalHits > 0) {
                SearchHits hits = searchResponse.getHits();
                for (SearchHit hit : hits) {
                    UserInfo userInfo = JSON.parseObject(hit.getSourceAsString(), UserInfo.class);
                    log.info(userInfo.toString());
                }
                log.info("---------------------------------------------------------------");
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }
 
    
    /**
     * 词语匹配查询
     */
    public static void  matchPhraseQuery(String indexName) {
        try {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchPhraseQuery("address", "北京市通州区"));  //构建查询条件
            
            // 创建查询请求对象，将查询对象配置到其中
            SearchRequest searchRequest = new SearchRequest(indexName);
            searchRequest.source(searchSourceBuilder);
            
            // 执行查询，根据状态和数据条数验证是否返回了数据
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
            if (RestStatus.OK.equals(searchResponse.status()) && searchResponse.getHits().totalHits > 0) {
                SearchHits hits = searchResponse.getHits();
                for (SearchHit hit : hits) {
                    UserInfo userInfo = JSON.parseObject(hit.getSourceAsString(), UserInfo.class);
                    log.info(userInfo.toString());
                }
                log.info("---------------------------------------------------------------");
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }
 
    /**
     * 内容在多字段中进行查询
     */
    public static void matchMultiQuery(String indexName) {
        try {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.multiMatchQuery("北京市", "address", "remark"));  //构建查询条件
            // 创建查询请求对象，将查询对象配置到其中
            SearchRequest searchRequest = new SearchRequest(indexName);
            searchRequest.source(searchSourceBuilder);
            
            // 执行查询，根据状态和数据条数验证是否返回了数据
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
            if (RestStatus.OK.equals(searchResponse.status()) && searchResponse.getHits().totalHits > 0) {
                SearchHits hits = searchResponse.getHits();
                for (SearchHit hit : hits) {
                    UserInfo userInfo = JSON.parseObject(hit.getSourceAsString(), UserInfo.class);
                    log.info(userInfo.toString());
                }
                log.info("---------------------------------------------------------------");
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }



    
    /**
     * 模糊查询所有以 “三” 结尾的姓名
     */
    public static void fuzzyQuery(String indexName){
        try {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.fuzzyQuery("name", "三").fuzziness(Fuzziness.AUTO)); //构建查询条件
            
            // 创建查询请求对象，将查询对象配置到其中
            SearchRequest searchRequest = new SearchRequest(indexName);
            searchRequest.source(searchSourceBuilder);
            
            // 执行查询，根据状态和数据条数验证是否返回了数据
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
            if (RestStatus.OK.equals(searchResponse.status()) && searchResponse.getHits().totalHits > 0) {
                SearchHits hits = searchResponse.getHits();
                for (SearchHit hit : hits) {
                    UserInfo userInfo = JSON.parseObject(hit.getSourceAsString(), UserInfo.class);
                    log.info(userInfo.toString());
                }
                log.info("---------------------------------------------------------------");
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }

    
    /**
     * 查询岁数 ≥ 30 岁的员工数据
     */
    public static void rangeQuery(String indexName) {
        try {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.rangeQuery("age").gte(30)); //构建查询条件
            
            // 创建查询请求对象，将查询对象配置到其中
            SearchRequest searchRequest = new SearchRequest(indexName);
            searchRequest.source(searchSourceBuilder);
            
            // 执行查询，根据状态和数据条数验证是否返回了数据
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
            if (RestStatus.OK.equals(searchResponse.status()) && searchResponse.getHits().totalHits > 0) {
                SearchHits hits = searchResponse.getHits();
                for (SearchHit hit : hits) {
                    UserInfo userInfo = JSON.parseObject(hit.getSourceAsString(), UserInfo.class);
                    log.info(userInfo.toString());
                }
                log.info("---------------------------------------------------------------");
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }
    
    

    
    /**
     * 查询所有以 “三” 结尾的姓名
     *
     * *：表示多个字符（0个或多个字符）
     * ?：表示单个字符
     */
    public static void wildcardQuery(String indexName) {
        try {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.wildcardQuery("name.keyword", "*三")); //构建查询条件
            
            // 创建查询请求对象，将查询对象配置到其中
            SearchRequest searchRequest = new SearchRequest(indexName);
            searchRequest.source(searchSourceBuilder);
            
            // 执行查询，根据状态和数据条数验证是否返回了数据
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
            if (RestStatus.OK.equals(searchResponse.status()) && searchResponse.getHits().totalHits > 0) {
                SearchHits hits = searchResponse.getHits();
                for (SearchHit hit : hits) {
                    UserInfo userInfo = JSON.parseObject(hit.getSourceAsString(), UserInfo.class);
                    log.info(userInfo.toString());
                }
                log.info("---------------------------------------------------------------");
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }

    

    //Bool查询对应Lucene中的BooleanQuery，它由一个或者多个子句组成，每个子句都有特定的类型。
    public static void boolQuery(String indexName) {
        try {
            //创建 Bool查询构建器，并构建查询条件
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            boolQueryBuilder.must(QueryBuilders.termsQuery("address.keyword", "北京市昌平区", "北京市大兴区", "北京市房山区"))
                    .filter().add(QueryBuilders.rangeQuery("birthDate").format("yyyy").gte("1990").lte("1995"));
            
            // 构建查询源构建器
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(boolQueryBuilder);
            
            // 创建查询请求对象，将查询对象配置到其中
            SearchRequest searchRequest = new SearchRequest(indexName);
            searchRequest.source(searchSourceBuilder);
            
            // 执行查询，根据状态和数据条数验证是否返回了数据
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
            if (RestStatus.OK.equals(searchResponse.status()) && searchResponse.getHits().totalHits > 0) {
                SearchHits hits = searchResponse.getHits();
                for (SearchHit hit : hits) {
                    UserInfo userInfo = JSON.parseObject(hit.getSourceAsString(), UserInfo.class);
                    log.info(userInfo.toString());
                }
                log.info("---------------------------------------------------------------");
            }
        }catch (IOException e){
            log.error("",e);
        }
    }


}
