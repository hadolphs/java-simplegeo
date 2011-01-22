package com.simplegeo.client;

import java.io.IOException;

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;

import com.simplegeo.client.callbacks.SimpleGeoCallback;
import com.simplegeo.client.handler.JSONHandler;
import com.simplegeo.client.handler.SimpleGeoJSONHandler;
import com.simplegeo.client.http.SimpleGeoHandler;

public class SimpleGeoNearbyAddressClient extends AbstractSimpleGeoClient
{
    protected static SimpleGeoNearbyAddressClient sharedNearbyAddressService = null;

    /**
     * Method that ensures we only have one instance of the SimpleGeoNearbyAddressClient instantiated and allows server connection variables to be overridden.
     * 
     * @param baseUrl String api.simplegeo.com is default, but can be overridden.
     * @param port String 80 is default, but can be overridden.
     * @param apiVersion String 1.0 is default, but can be overridden.
     * @return SimpleGeoNearbyAddressClient
     */
    public static SimpleGeoNearbyAddressClient getInstance(String baseUrl, String port, String apiVersion)
    {
        if (sharedNearbyAddressService == null)
        {
            sharedNearbyAddressService = new SimpleGeoNearbyAddressClient(baseUrl, port, apiVersion);
        }

        return (SimpleGeoNearbyAddressClient) sharedNearbyAddressService;
    }

    /**
     * Default method for retrieving a SimpleGeoNearbyAddressClient.
     * 
     * @return SimpleGeoNearbyAddressClient
     */
    public static SimpleGeoNearbyAddressClient getInstance()
    {
        // return getInstance(DEFAULT_HOST, DEFAULT_PORT, DEFAULT_VERSION);
        return getInstance(DEFAULT_HOST, DEFAULT_PORT, "0.1");
    }

    /**
     * SimpleGeoNearbyAddressClient constructor
     * 
     * @param baseUrl String api.simplegeo.com is default, but can be overridden.
     * @param port String 80 is default, but can be overridden.
     * @param apiVersion String 1.0 is default, but can be overridden.
     */
    private SimpleGeoNearbyAddressClient(String baseUrl, String port, String apiVersion)
    {
        super(baseUrl, port, apiVersion);
        endpoints.put("nearbyAddress", "nearby/address/%s,%s.json");
    }

    /**
     * 
     * @return
     * @throws IOException
     */
    public Object getEndpointDescriptions() throws IOException
    {
        return this.executeGet(String.format(this.getEndpoint("endpoints")), new JSONHandler());
    }

    /**
     * Search for nearby places.
     * 
     * @param lat Double latitude.
     * @param lon Double longitude.
     * @return FutureTask/FeatureCollection FutureTask if supported, else a FeatureCollection containing search results.
     * @throws IOException
     */
    public Object getNearbyAddress(double lat, double lon) throws IOException
    {
        return this.executeGet(String.format(this.getEndpoint("nearbyAddress"), lat, lon), new JSONHandler());
    }

    @Override
    protected Object executeGet(String uri, SimpleGeoJSONHandler handler) throws IOException
    {
        HttpGet get = new HttpGet(uri);
        return super.execute(get, new SimpleGeoHandler(handler));
    }

    @Override
    protected Object executePost(String uri, String jsonPayload, SimpleGeoJSONHandler handler) throws IOException
    {
        HttpPost post = new HttpPost(uri);
        post.setEntity(new ByteArrayEntity(jsonPayload.getBytes()));
        post.addHeader("Content-type", "application/json");
        return super.execute(post, new SimpleGeoHandler(handler));
    }

    @Override
    protected Object executePut(String uri, String jsonPayload, SimpleGeoJSONHandler handler) throws IOException
    {
        HttpPut put = new HttpPut(uri);
        put.setEntity(new ByteArrayEntity(jsonPayload.getBytes()));
        put.addHeader("Content-type", "application/json");
        return super.execute(new HttpPut(uri), new SimpleGeoHandler(handler));
    }

    @Override
    protected Object executeDelete(String uri, SimpleGeoJSONHandler handler) throws IOException
    {
        return super.execute(new HttpDelete(uri), new SimpleGeoHandler(handler));
    }

    @Override
    protected void executeDelete(String uri, SimpleGeoJSONHandler handler, SimpleGeoCallback callback) throws IOException
    {
        super.execute(new HttpDelete(uri), new SimpleGeoHandler(handler), callback);
    }

    @Override
    protected void executeGet(String uri, SimpleGeoJSONHandler handler, SimpleGeoCallback callback) throws IOException
    {
        uri = this.removeEmptyParameters(uri);
        HttpGet get = new HttpGet(uri);
        super.execute(get, new SimpleGeoHandler(handler), callback);
    }

    @Override
    protected void executePost(String uri, String jsonPayload, SimpleGeoJSONHandler handler, SimpleGeoCallback callback) throws IOException
    {
        HttpPost post = new HttpPost(uri);
        post.setEntity(new ByteArrayEntity(jsonPayload.getBytes()));
        post.addHeader("Content-type", "application/json");
        super.execute(post, new SimpleGeoHandler(handler), callback);
    }

    @Override
    protected void executePut(String uri, String jsonPayload, SimpleGeoJSONHandler handler, SimpleGeoCallback callback) throws IOException
    {
        HttpPut put = new HttpPut(uri);
        put.setEntity(new ByteArrayEntity(jsonPayload.getBytes()));
        put.addHeader("Content-type", "application/json");
        super.execute(new HttpPut(uri), new SimpleGeoHandler(handler), callback);
    }

    /**
     * Remove empty parameters so we're not sending q=&category=.
     * 
     * @param uri String uri containing parameters
     * @return String uri with empty parameters removed
     */
    private String removeEmptyParameters(String uri)
    {
        if (uri.indexOf("?") == -1)
            return uri;

        String base = uri.substring(0, uri.indexOf("?"));
        String[] parameters = uri.substring(uri.indexOf("?") + 1).split("&");
        String newQuery = "";
        for (String parameter : parameters)
        {
            if (!parameter.endsWith("="))
            {
                newQuery += "&" + parameter;
            }
        }
        return base + "?" + newQuery.replaceFirst("&", "");
    }
}
