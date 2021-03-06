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
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
public class Case1 {
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
        // All registered user who has played a particular content in a specified date range

        RangeQueryBuilder queryDate = QueryBuilders.rangeQuery("eventdate").gte("01/04/2017").lte("19/04/2017").format("dd/MM/yyyy||dd/MM/yyyy");

        BoolQueryBuilder query = QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("event", "contentPlay"))
                //.must(QueryBuilders.termQuery("status", "registered"))
                .must(QueryBuilders.matchQuery("eventnumbers","422"))
                .must(queryDate);
//
        SearchResponse preResponse = client.prepareSearch("eventsbig").setQuery(query).get();

        for (SearchHit hit : preResponse.getHits()){
            Map map = hit.getSource();
            userList.add(Integer.parseInt(map.get("userid").toString()));
        }
        System.out.println("userList>>"+userList);

        BoolQueryBuilder queryUser = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("status", "anonymous"))
                .must(QueryBuilders.termsQuery("userid",userList));


        SearchResponse response = client.prepareSearch("userbig")
                .setSearchType(SearchType.DEFAULT)
                .setQuery(queryUser)
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
                .get();


        System.out.println("Query Time  "+ (System.currentTimeMillis()-timeNow));

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
