package com.elasticsearch.service;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.elasticsearch.tools.ElasticSearchTools;

public class SearchService2 {
	
	private static final Logger log = LoggerFactory.getLogger(SearchService2.class);
	private static String indexName = "user_gptweet_list";
    
    public static void main(String[] args) throws IOException{
    	
    	searchByKeyWord("gptweet_content","理财","gptweet_content", 0 , 3);
    }
    
    
	//通过关键字查找match query:搜索时首先会解析查询字符串，进行分词，然后查询，
	public static void searchByKeyWord(String fieldName,String keyWord, String highlightField, int start, int size) {
		RestHighLevelClient hightLevelClient = ElasticSearchTools.getClient2();
        MatchQueryBuilder queryBuilder = new MatchQueryBuilder(fieldName,keyWord); //match query 模糊匹配
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(queryBuilder);
        searchSourceBuilder.from(start); //设置起始
        searchSourceBuilder.size(size); //设置返回数量
        
        HighlightBuilder highlightBuilder = new HighlightBuilder(); //初始化高亮对象
        highlightBuilder.field(highlightField).fragmentSize(180).numOfFragments(1);//高亮字段,字符数量，返回数量
        highlightBuilder.preTags("<span style='color:red'>").postTags("</span>");//高亮标签
        searchSourceBuilder.highlighter(highlightBuilder);
        
	    //开启搜索_source里面的内容
	    searchSourceBuilder.fetchSource(true);
	    String[] excludeFields = new String[] {"recommend_from", "gptweet_ip","group_type","","post_time"};  //需要过滤的属性
	    searchSourceBuilder.fetchSource(null, excludeFields);
        
		SearchRequest searchRequest = new SearchRequest(indexName);
		searchRequest.source(searchSourceBuilder);
	    searchSourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));//按照score降序排序
         
		try{
			SearchResponse searchResponse = hightLevelClient.search(searchRequest);
			
	    	//状态结果
	    	RestStatus status = searchResponse.status();
	    	TimeValue took = searchResponse.getTook();
	    	boolean timedOut = searchResponse.isTimedOut();
	    	System.out.println("状态码:" + status.toString() + "  花费时间:" + took + "  超时:" + timedOut);
	    	
	    	//分片情况
	        int totalShards = searchResponse.getTotalShards();
	        int successfulShards = searchResponse.getSuccessfulShards();
	        int failedShards = searchResponse.getFailedShards();
	        System.out.println("全部分片:" + totalShards + "  成功分片:" + successfulShards + "  失败分片:" + failedShards);
	    	
	        //获取结果
	        SearchHits hits = searchResponse.getHits();
	        long totalHits = hits.getTotalHits();
	    	float maxScore = hits.getMaxScore();
	    	System.out.println("查到结果数:" + totalHits + "  最高得分:" + maxScore);
	    	
	        SearchHit[] searchHits = hits.getHits();
	        for (SearchHit searchHit : searchHits) {
	            System.out.println("分数:" + searchHit.getScore() + "  结果:" + searchHit.getSourceAsString());
	            System.out.println("<br>");
	            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
	        	Map<String, Object> sourceMap = searchHit.getSourceAsMap();
	            
	            Map<String, HighlightField> highlightMap = searchHit.getHighlightFields();
	            System.out.println("highlightMap:" + highlightMap);
	            System.out.println("<br>");
	            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

	            HighlightField highlightValue = highlightMap.get(highlightField);
	            if(highlightValue != null){ //要判断是不是为空,不然匹配的结果没有高亮内容,会报空指针异常
	                Text[] fragments = highlightValue.fragments();  
	                String name = "";
	                for (Text text : fragments) {
	                	name += text;
	                }
	                sourceMap.put(highlightField, name); //高亮字段替换掉原本的内容
	            }
	            System.out.println(sourceMap.toString());
	        }
		}catch(IOException e){
			e.printStackTrace();
		}finally{
			ElasticSearchTools.closeClient(hightLevelClient);
		}
    }

}
