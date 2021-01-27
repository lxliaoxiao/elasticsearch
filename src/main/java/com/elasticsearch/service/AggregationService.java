package com.elasticsearch.service;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.ParsedAvg;
import org.elasticsearch.search.aggregations.metrics.max.ParsedMax;
import org.elasticsearch.search.aggregations.metrics.min.ParsedMin;
import org.elasticsearch.search.aggregations.metrics.percentiles.ParsedPercentiles;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentile;
import org.elasticsearch.search.aggregations.metrics.stats.ParsedStats;
import org.elasticsearch.search.aggregations.metrics.sum.ParsedSum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.ParsedTopHits;
import org.elasticsearch.search.aggregations.metrics.valuecount.ParsedValueCount;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.elasticsearch.tools.ElasticSearchTools;

public class AggregationService {

	private static final Logger log = LoggerFactory.getLogger(AggregationService.class);
    private static RestHighLevelClient restHighLevelClient = ElasticSearchTools.getClient2();
    
    public static void main(String[] args) throws IOException{
    	String indexName = "test-user";
    	
    	//Metric指标聚合
//    	aggregationStats(indexName);
//    	aggregationMin(indexName);
//    	aggregationMax(indexName);
//    	aggregationAvg(indexName);
//    	aggregationSum(indexName);
//    	aggregationCount(indexName);
//    	aggregationPercentiles(indexName);
    	
    	//Bucket桶分聚合
//    	aggrBucketTerms(indexName);
//    	aggrBucketRange(indexName);
//    	aggrBucketDateRange(indexName);
//    	aggrBucketHistogram(indexName);
//    	aggrBucketDateHistogram(indexName);
    	
    	//Metric与 Bucket聚合分析
    	aggregationTopHits(indexName);
    	
    	ElasticSearchTools.closeClient(restHighLevelClient);
    }
    
    
    /**
     * stats 统计员工总数、员工工资最高值、员工工资最低值、员工平均工资、员工工资总和
     */
    public static Object aggregationStats(String indexName) {
        String responseResult = "";
        try {
            AggregationBuilder aggr = AggregationBuilders.stats("salary_stats").field("salary"); //设置聚合条件
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.aggregation(aggr);
            searchSourceBuilder.size(0); //设置查询结果不返回，只返回聚合结果
            SearchRequest request = new SearchRequest(indexName);
            request.source(searchSourceBuilder);
            
            // 执行请求,获取响应中的聚合信息
            SearchResponse response = restHighLevelClient.search(request);
            Aggregations aggregations = response.getAggregations();
            if (RestStatus.OK.equals(response.status()) || aggregations != null) {
                ParsedStats aggregation = aggregations.get("salary_stats"); //转换为Stats对象
                log.info("-------------------------------------------");
                log.info("统计聚合信息:");
                log.info("count：{}", aggregation.getCount());
                log.info("avg salary：{}", aggregation.getAvg());
                log.info("max salary：{}", aggregation.getMax());
                log.info("min salary：{}", aggregation.getMin());
                log.info("sum salary：{}", aggregation.getSum());
                log.info("-------------------------------------------");
            }
            responseResult = response.toString();
        } catch (IOException e) {
            log.error("", e);
        }
        return responseResult;
    }
    

    /**
     * min 统计员工工资最低值
     */
    public static Object aggregationMin(String indexName) {
        String responseResult = "";
        try {
            // 设置聚合条件
            AggregationBuilder aggr = AggregationBuilders.min("salary_min").field("salary");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.aggregation(aggr);
            searchSourceBuilder.size(0);
            SearchRequest request = new SearchRequest(indexName);
            request.source(searchSourceBuilder);
            
            // 执行请求,获取响应中的聚合信息
            SearchResponse response = restHighLevelClient.search(request);
            Aggregations aggregations = response.getAggregations();
            if (RestStatus.OK.equals(response.status()) || aggregations != null) {
                ParsedMin aggregation = aggregations.get("salary_min"); //转换为 Min 对象
                log.info("-------------------------------------------");
                log.info("最小值聚合信息:");
                log.info("min salary：{}", aggregation.getValue());
                log.info("-------------------------------------------");
            }
            responseResult = response.toString();
        } catch (IOException e) {
            log.error("", e);
        }
        return responseResult;
    }
 
    /**
     * max 统计员工工资最高值
     */
    public static Object aggregationMax(String indexName) {
        String responseResult = "";
        try {
            //设置聚合条件
            AggregationBuilder aggr = AggregationBuilders.max("salary_max").field("salary");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.aggregation(aggr);
            searchSourceBuilder.size(0);
            SearchRequest request = new SearchRequest(indexName);
            request.source(searchSourceBuilder);
            
            // 执行请求,获取响应中的聚合信息
            SearchResponse response = restHighLevelClient.search(request);
            Aggregations aggregations = response.getAggregations();
            if (RestStatus.OK.equals(response.status()) || aggregations != null) {
                ParsedMax aggregation = aggregations.get("salary_max"); //转换为 Max 对象
                log.info("-------------------------------------------");
                log.info("最大值聚合信息:");
                log.info("max salary：{}", aggregation.getValue());
                log.info("-------------------------------------------");
            }
            responseResult = response.toString();
        } catch (IOException e) {
            log.error("", e);
        }
        return responseResult;
    }
 
    /**
     * avg 统计员工工资平均值
     */
    public static Object aggregationAvg(String indexName) {
        String responseResult = "";
        try {
            // 设置聚合条件
            AggregationBuilder aggr = AggregationBuilders.avg("salary_avg").field("salary");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.aggregation(aggr);
            searchSourceBuilder.size(0);
            SearchRequest request = new SearchRequest(indexName);
            request.source(searchSourceBuilder);
            
            // 执行请求，获取响应中的聚合信息
            SearchResponse response = restHighLevelClient.search(request);
            Aggregations aggregations = response.getAggregations();
            if (RestStatus.OK.equals(response.status()) || aggregations != null) {
                ParsedAvg aggregation = aggregations.get("salary_avg"); //转换为 Avg 对象
                log.info("-------------------------------------------");
                log.info("均值聚合信息:");
                log.info("avg salary：{}", aggregation.getValue());
                log.info("-------------------------------------------");
            }
            responseResult = response.toString();
        } catch (IOException e) {
            log.error("", e);
        }
        return responseResult;
    }
 
    /**
     * sum 统计员工工资总值
     */
    public static Object aggregationSum(String indexName) {
        String responseResult = "";
        try {
            // 设置聚合条件
            SumAggregationBuilder aggr = AggregationBuilders.sum("salary_sum").field("salary");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.aggregation(aggr);
            searchSourceBuilder.size(0);
            SearchRequest request = new SearchRequest(indexName);
            request.source(searchSourceBuilder);
            
            // 执行请求，获取响应中的聚合信息
            SearchResponse response = restHighLevelClient.search(request);
            Aggregations aggregations = response.getAggregations();
            if (RestStatus.OK.equals(response.status()) || aggregations != null) {
                ParsedSum aggregation = aggregations.get("salary_sum"); //转换为 Sum 对象
                log.info("-------------------------------------------");
                log.info("求和聚合信息:");
                log.info("sum salary：{}", String.valueOf((aggregation.getValue())));
                log.info("-------------------------------------------");
            }
            responseResult = response.toString();
        } catch (IOException e) {
            log.error("", e);
        }
        return responseResult;
    }
 
    /**
     * count 统计员工总数
     */
    public static Object aggregationCount(String indexName) {
        String responseResult = "";
        try {
            // 设置聚合条件
            AggregationBuilder aggr = AggregationBuilders.count("employee_count").field("salary");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.aggregation(aggr);
            searchSourceBuilder.size(0);
            SearchRequest request = new SearchRequest(indexName);
            request.source(searchSourceBuilder);
            
            //执行请求，获取响应中的聚合信息
            SearchResponse response = restHighLevelClient.search(request);
            Aggregations aggregations = response.getAggregations();
            if (RestStatus.OK.equals(response.status()) || aggregations != null) {
                ParsedValueCount aggregation = aggregations.get("employee_count"); //转换为 ValueCount 对象
                log.info("-------------------------------------------");
                log.info("值计数聚合信息:");
                log.info("count：{}", aggregation.getValue());
                log.info("-------------------------------------------");
            }
            responseResult = response.toString();
        } catch (IOException e) {
            log.error("", e);
        }
        return responseResult;
    }
 
    /**
     * percentiles 统计员工工资百分位
     */
    public static Object aggregationPercentiles(String indexName) {
        String responseResult = "";
        try {
            //设置聚合条件
            AggregationBuilder aggr = AggregationBuilders.percentiles("salary_percentiles").field("salary");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.aggregation(aggr);
            searchSourceBuilder.size(0);
            SearchRequest request = new SearchRequest(indexName);
            request.source(searchSourceBuilder);
            
            // 执行请求，获取响应中的聚合信息
            SearchResponse response = restHighLevelClient.search(request);
            Aggregations aggregations = response.getAggregations();
            if (RestStatus.OK.equals(response.status()) || aggregations != null) {
                ParsedPercentiles aggregation = aggregations.get("salary_percentiles"); //转换为 Percentiles 对象
                log.info("-------------------------------------------");
                log.info("百分比聚合信息:");
                for (Percentile percentile : aggregation) {
                    log.info("salary百分位：{}：{}", percentile.getPercent(), percentile.getValue());
                }
                log.info("-------------------------------------------");
            }
            responseResult = response.toString();
        } catch (IOException e) {
            log.error("", e);
        }
        return responseResult;
    }

    
    
    /**
     * 按年龄进行 字段聚合分桶
     */
    public static void aggrBucketTerms(String indexName) {
        try {
        	//设置聚合条件
            AggregationBuilder aggr = AggregationBuilders.terms("age_bucket").field("age");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.size(10);
            searchSourceBuilder.aggregation(aggr);
            SearchRequest request = new SearchRequest(indexName);
            request.source(searchSourceBuilder);
            
            // 执行请求，获取响应中的聚合信息
            SearchResponse response = restHighLevelClient.search(request);
            Aggregations aggregations = response.getAggregations();
            if (RestStatus.OK.equals(response.status())) {
                Terms byCompanyAggregation = aggregations.get("age_bucket");
                List<? extends Terms.Bucket> buckets = byCompanyAggregation.getBuckets();
                log.info("-------------------------------------------");
                log.info("年龄字段分桶聚合信息:");
                for (Terms.Bucket bucket : buckets) {
                    log.info("桶名：{} | 总数：{}", bucket.getKeyAsString(), bucket.getDocCount());
                }
                log.info("-------------------------------------------");
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }
 
    /**
     * 按工资进行 范围聚合分桶
     */
    public static void aggrBucketRange(String indexName) {
        try {
        	//设置聚合条件
            AggregationBuilder aggr = AggregationBuilders.range("salary_range_bucket")
                    .field("salary")
                    .addUnboundedTo("低级员工", 3000)
                    .addRange("中级员工", 5000, 9000)
                    .addUnboundedFrom("高级员工", 9000);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.size(0);
            searchSourceBuilder.aggregation(aggr);
            SearchRequest request = new SearchRequest(indexName);
            request.source(searchSourceBuilder);
            
            // 执行请求，获取响应中的聚合信息
            SearchResponse response = restHighLevelClient.search(request);
            Aggregations aggregations = response.getAggregations();
            if (RestStatus.OK.equals(response.status())) {
                Range byCompanyAggregation = aggregations.get("salary_range_bucket");
                List<? extends Range.Bucket> buckets = byCompanyAggregation.getBuckets();
                log.info("-------------------------------------------");
                log.info("工资范围分桶聚合信息:");
                for (Range.Bucket bucket : buckets) {
                    log.info("桶名：{} | 总数：{}", bucket.getKeyAsString(), bucket.getDocCount());
                }
                log.info("-------------------------------------------");
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }
 
    /**
     * 按生日进行 日期范围聚合分桶
     */
    public static void  aggrBucketDateRange(String indexName) {
        try {
        	//设置聚合条件
            AggregationBuilder aggr = AggregationBuilders.dateRange("date_range_bucket")
                    .field("birthDate")
                    .format("yyyy")
                    .addRange("1985-1990", "1985", "1990")
                    .addRange("1990-1995", "1990", "1995");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.size(0);
            searchSourceBuilder.aggregation(aggr);
            SearchRequest request = new SearchRequest(indexName);
            request.source(searchSourceBuilder);
            
            // 执行请求，获取响应中的聚合信息
            SearchResponse response = restHighLevelClient.search(request);
            Aggregations aggregations = response.getAggregations();
            if (RestStatus.OK.equals(response.status())) {
                Range byCompanyAggregation = aggregations.get("date_range_bucket");
                List<? extends Range.Bucket> buckets = byCompanyAggregation.getBuckets();
                log.info("-------------------------------------------");
                log.info("生日日期范围分桶聚合信息:");
                for (Range.Bucket bucket : buckets) {
                    log.info("桶名：{} | 总数：{}", bucket.getKeyAsString(), bucket.getDocCount());
                }
                log.info("-------------------------------------------");
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }
 
    /**
     * 按工资进行 直方图聚合分桶
     */
    public static void aggrBucketHistogram(String indexName) {
        try {
        	//设置聚合条件
            AggregationBuilder aggr = AggregationBuilders.histogram("salary_histogram").field("salary")
                    .extendedBounds(0, 12000)
                    .interval(3000);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.size(0);
            searchSourceBuilder.aggregation(aggr);
            SearchRequest request = new SearchRequest(indexName);
            request.source(searchSourceBuilder);
            
            // 执行请求，获取响应中的聚合信息
            SearchResponse response = restHighLevelClient.search(request);
            Aggregations aggregations = response.getAggregations();
            if (RestStatus.OK.equals(response.status())) {
                Histogram byCompanyAggregation = aggregations.get("salary_histogram");
                List<? extends Histogram.Bucket> buckets = byCompanyAggregation.getBuckets();
                log.info("-------------------------------------------");
                log.info("工资直方图分桶聚合信息:");
                for (Histogram.Bucket bucket : buckets) {
                    log.info("桶名：{} | 总数：{}", bucket.getKeyAsString(), bucket.getDocCount());
                }
                log.info("-------------------------------------------");
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }
 
    /**
     * 按生日进行 日期直方图聚合分桶
     */
    public static void aggrBucketDateHistogram(String indexName) {
        try {
        	//设置聚合条件
            AggregationBuilder aggr = AggregationBuilders.dateHistogram("birthday_histogram")
                    .field("birthDate")
                    .interval(1)
                    .dateHistogramInterval(DateHistogramInterval.YEAR)
                    .format("yyyy");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.size(0);
            searchSourceBuilder.aggregation(aggr);
            SearchRequest request = new SearchRequest(indexName);
            request.source(searchSourceBuilder);
            
            // 执行请求，获取响应中的聚合信息
            SearchResponse response = restHighLevelClient.search(request);
            Aggregations aggregations = response.getAggregations();
            if (RestStatus.OK.equals(response.status())) {
                Histogram byCompanyAggregation = aggregations.get("birthday_histogram");
                List<? extends Histogram.Bucket> buckets = byCompanyAggregation.getBuckets();
                log.info("-------------------------------------------");
                log.info("生日日期直方图分桶聚合信息:");
                for (Histogram.Bucket bucket : buckets) {
                    log.info("桶名：{} | 总数：{}", bucket.getKeyAsString(), bucket.getDocCount());
                }
                log.info("-------------------------------------------");
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }
    
    

    
    /**
     * 先按年龄分桶、然后统计每个员工工资最高值
     */
    public static void aggregationTopHits(String indexName) {
        try {
            AggregationBuilder topSalary = AggregationBuilders.topHits("salary_max_user").size(1).sort("salary", SortOrder.DESC);
            AggregationBuilder ageBucket = AggregationBuilders.terms("salary_bucket").field("age").size(10);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.size(0); //如果只想返回聚合统计结果，不想返回其他查询结果，可以将分页大小设置为0
            searchSourceBuilder.aggregation(ageBucket.subAggregation(topSalary)); //子聚合（先按年龄分桶、然后统计每个员工工资最高值）
            SearchRequest request = new SearchRequest(indexName);
            request.source(searchSourceBuilder);
            
            // 执行请求，获取响应中的聚合信息
            SearchResponse response = restHighLevelClient.search(request);
            Aggregations aggregations = response.getAggregations();
            if (RestStatus.OK.equals(response.status())) {
                Terms byAgeAggregation = aggregations.get("salary_bucket");
                List<? extends Terms.Bucket> buckets = byAgeAggregation.getBuckets();
                log.info("-------------------------------------------");
                log.info("聚合信息:");
                for (Terms.Bucket bucket : buckets) {
                    log.info("桶名：{}", bucket.getKeyAsString());
                    ParsedTopHits topHits = bucket.getAggregations().get("salary_max_user");
                    for (SearchHit hit:topHits.getHits()){
                        log.info(hit.getSourceAsString());
                    }
                }
                log.info("-------------------------------------------");
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }



}
