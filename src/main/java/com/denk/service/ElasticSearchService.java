package com.denk.service;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.util.Collections;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by lvdengke1 on 2017/9/23.
 */
public class ElasticSearchService {

    private RestClientBuilder restClientBuilder;

    public ElasticSearchService(HttpHost[] httpHosts) {
        this.restClientBuilder = RestClient.builder(httpHosts);
        this.restClientBuilder.setFailureListener(new RestClient.FailureListener() {
            @Override
            public void onFailure(HttpHost host) {
                Logger.getLogger(ElasticSearchService.class.getName())
                        .log(Level.SEVERE, "Host:{0} Port:{1} Fail...", new Object[]{host.getHostName(), host.getPort()});
            }
        });
    }

    public Response get(String endpoint, Map<String, String> params) throws Exception {
        try (RestClient rc = this.restClientBuilder.build()) {
            if (params == null) {
                return rc.performRequest(HttpGet.METHOD_NAME, endpoint);
            }
            return rc.performRequest(HttpGet.METHOD_NAME, endpoint, params);
        }
    }

    public Response put(String endpoint, Map<String, String> params, HttpEntity entity) throws Exception {
        try (RestClient rc = this.restClientBuilder.build()) {
            return rc.performRequest(HttpPut.METHOD_NAME, endpoint, params, entity);
        }
    }

    public Response delete(String endpoint) throws Exception {
        try (RestClient rc = this.restClientBuilder.build()) {
            return rc.performRequest(HttpDelete.METHOD_NAME, endpoint);
        }
    }

    public static void main(String[] args) throws Exception {
        HttpHost[] hs = new HttpHost[]{new HttpHost("127.0.0.1", 9200)};
        ElasticSearchService ess = new ElasticSearchService(hs);
        //testPut(ess);
        testGet(ess);
        //testDelete(ess);
    }

    public static void testGet(ElasticSearchService ess) throws Exception {
        System.out.println(EntityUtils.toString(ess.get("/posts/_search", Collections.singletonMap("pretty", "true")).getEntity()));
    }

    public static void testPut(ElasticSearchService ess) throws Exception {
        String jsonString = "{" +
                "\"user\":\"denk\"," +
                "\"postDate\":\"2017-09-25\"," +
                "\"message\":\"trying out Elasticsearch again\"" +
                "}";
        HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_ATOM_XML.APPLICATION_JSON);
        System.out.println(EntityUtils.toString(ess.put("/posts/doc/1", Collections.singletonMap("pretty", "true"), entity).getEntity()));
    }

    public static void testDelete(ElasticSearchService ess) throws Exception {
        System.out.println(EntityUtils.toString(ess.delete("posts/doc/1").getEntity()));
    }
}
