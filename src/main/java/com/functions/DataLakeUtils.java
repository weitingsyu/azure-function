package com.functions;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.TimeZone;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

public class DataLakeUtils {
    public static final String storageConnectionString = "DefaultEndpointsProtocol=https;"
            + "AccountName=wsdcdmstorageprd;"
            + "AccountKey=nIg3SippxnZ1jmsy8AVhyrwXuWjWAFFMvoGBMwXxLue3Kpmy104lTE1GSyUyN0g4o6gRNmPISI78915MS8mu5A==";

    public static boolean appendFile(String csvFilePath, String nameSpace, String name, String content)
            throws Exception {
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {

            String key = "umaWr+Rp+MZOQhes1W95r1bwrJl2jF+254oToYhOSR9r3VUvaNwJQpWocLc+LUpGgxdJtjeBMzwVs2Ox1fk9gQ==";
            String storageAccount = "ssotstoragepoc";
            String str = "hello world";
            HttpPut httpPut = new HttpPut(
                    "https://" + storageAccount + ".dfs.core.windows.net/eventhub/wistronssoteventhub/hello2.txt");

            String gmDate2 = getStorageDate();
            String stringToSign = "PUT\n" + "\n" // content encoding
                    + "\n" // content language
                    + "\n"// content length
                    + "\n" // content md5
                    + "text/plain" + "\n" // content type
                    + "\n" // date
                    + "\n" // if modified since
                    + "\n" // if match
                    + "\n" // if none match
                    + "\n" // if unmodified since
                    + "\n" // range
                    + "x-ms-date:" + gmDate2 + "\nx-ms-version:2018-11-09\n"// "x-ms-client-request-id:"+requestId
                    + "/" + storageAccount + "/wistronssoteventhub/hello2.txt" // resources +
                    + "\naction:flush\nposition:" + str.length(); // resources

            httpPut.setHeader("x-ms-version", "2018-11-09");
            httpPut.setHeader("x-ms-date", gmDate2);
            String authorizationStringToHash = getAuthenticationString(storageAccount, key, stringToSign);

            httpPut.setHeader("Authorization", authorizationStringToHash);
            httpPut.setEntity(new StringEntity(str));

            ArrayList<NameValuePair> postParameters;
            postParameters = new ArrayList<NameValuePair>();
            postParameters.add(new BasicNameValuePair("action", "flush"));
            postParameters.add(new BasicNameValuePair("position", "" + str.length()));
            httpPut.setEntity(new UrlEncodedFormEntity(postParameters, "UTF-8"));

            System.out.println("Executing request " + httpPut.getRequestLine());

            // Create a custom response handler
            ResponseHandler<String> responseHandler = response -> {
                int status = response.getStatusLine().getStatusCode();
                HttpEntity entity = response.getEntity();
                String responStr = entity != null ? EntityUtils.toString(entity) : null;
                if (status >= 200 && status < 300) {
                    // HttpEntity entity = response.getEntity();
                    return responStr;
                } else {
                    System.out.println("====" + responStr);
                    throw new ClientProtocolException("Unexpected response status: " + status);
                }
            };
            String responseBody = httpclient.execute(httpPut, responseHandler);
            System.out.println("----------------------------------------");
            System.out.println(responseBody);
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        }
        return true;
    }

    public static String getStorageDate() {
        Date d = new Date(System.currentTimeMillis());
        SimpleDateFormat f = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz");
        f.setTimeZone(TimeZone.getTimeZone("GMT"));
        String dateString = f.format(d);
        return dateString;
    }

    private static String getAuthenticationString(String accountName, String accessKey, String stringToSign)
            throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(Base64.decodeBase64(accessKey), "HmacSHA256"));
        String authKey = new String(Base64.encodeBase64(mac.doFinal(stringToSign.getBytes("UTF-8"))));
        String auth = "SharedKey " + accountName + ":" + authKey;
        return auth;
    }
}