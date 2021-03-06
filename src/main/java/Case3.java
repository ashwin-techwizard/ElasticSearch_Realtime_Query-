/**
 * Created by YOGA710 on 4/19/2017.
 */

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
public class Case3 {
    public static double getPercentage(long n, long total) {
        double proportion = ((double) n) / ((double) total);
        return proportion * 100;
    }
    public static void main(String[] args) {

        Settings settings = Settings.builder()
                .put("cluster.name", "my-application").build();
        TransportClient client=null;
        try {
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
        }
        catch (Exception e){

        }



        long timeNow = System.currentTimeMillis();
        List<Integer> userList = new ArrayList<Integer>();

        //  Anonymous users between age 18 to 40, located in India or China or USA, who has done at least 10 content views

        BoolQueryBuilder query = QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("event", "contentView"));

        AggregationBuilder aggregation =
                AggregationBuilders
                        .terms("user").field("userid.keyword");
        SearchResponse preResponse1 = client.prepareSearch("eventsbig").setQuery(query)
            .addAggregation(aggregation)
                .get();


        Terms user = preResponse1.getAggregations().get("user");
        for (Terms.Bucket genderEntry : user.getBuckets()) {
            String key = genderEntry.getKey().toString();
            long docCount = genderEntry.getDocCount();

            System.out.println(key);
            System.out.println(docCount);
            if(docCount>1){
                userList.add(Integer.parseInt(key));
            }

        }



        System.out.println("userList>>"+userList);
        RangeQueryBuilder queryRange = QueryBuilders.rangeQuery("age").gte(18).lte(40);
        BoolQueryBuilder queryUser = QueryBuilders.boolQuery()
                //.must(QueryBuilders.termQuery("status", "anonymous"))
                .must(QueryBuilders.termsQuery("userid",userList))
                //.must(queryRange)
                //.must(QueryBuilders.termQuery("location", "UK"));
        .should(QueryBuilders.termQuery("status", "anonymous"))
                .should(QueryBuilders.termQuery("status", "subscribed")).minimumShouldMatch("1");
//                .should(QueryBuilders.termQuery("location", "India"))
//                .should(QueryBuilders.termQuery("location", "UK")).minimumShouldMatch("1");

        SearchResponse response = client.prepareSearch("userbig")
                //.setTypes("type1", "type2")
                .setSearchType(SearchType.DEFAULT)
                // .setQuery(QueryBuilders.matchQuery("userid", "1"))
                .setQuery(queryUser)
                //.setQuery(QueryBuilders.rangeQuery("EventResult").gt(60))
                .addAggregation(
                        AggregationBuilders
                                .terms("gender").field("gender.keyword"))
                .addAggregation(
                        AggregationBuilders
                                .terms("agegroup").field("agegroup.keyword"))
                .addAggregation(
                        AggregationBuilders
                                .terms("status").field("status.keyword"))
                .addAggregation(
                        AggregationBuilders
                                .terms("location").field("location.keyword"))
//                                .subAggregation(
//                                        AggregationBuilders.terms("gender").field("gender.keyword")
//                                )

                // .setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18))     // Filter
                // .setFrom(0).setSize(60).setExplain(true)
                .get();


        System.out.println("Query Time"+ (System.currentTimeMillis()-timeNow));

        long total=response.getHits().totalHits();
        System.out.println("Total "+total);
        Terms genderHits = response.getAggregations().get("gender");
        for (Terms.Bucket genderEntry : genderHits.getBuckets()) {
            String key = genderEntry.getKey().toString();
            long docCount = genderEntry.getDocCount();

            System.out.println(key);
            System.out.println(docCount);

        }

        Terms agegroupHits = response.getAggregations().get("agegroup");
        for (Terms.Bucket Entry : agegroupHits.getBuckets()) {
            String key = Entry.getKey().toString();
            long docCount = Entry.getDocCount();

            System.out.println(key);
            System.out.println(docCount);

        }

        Terms statusHits = response.getAggregations().get("status");
        for (Terms.Bucket Entry : statusHits.getBuckets()) {
            String key = Entry.getKey().toString();
            long docCount = Entry.getDocCount();

            System.out.println(key);
            System.out.println(docCount);

        }

        Terms locationHits = response.getAggregations().get("location");
        for (Terms.Bucket Entry : locationHits.getBuckets()) {
            String key = Entry.getKey().toString();
            long docCount = Entry.getDocCount();

            System.out.println(key);
            System.out.println(getPercentage(docCount,total)+" %");

        }



        client.close();
    }
}
