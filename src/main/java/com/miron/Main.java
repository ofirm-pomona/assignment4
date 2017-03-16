package com.miron;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {

    private static final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(1);

    public static void main(String[] args) throws Exception {
        // Construct a new Jest client according to configuration via factory
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder("https://search-assignment4-m6tgsiu4cgjntx4cp7zzo5ymky.us-west-1.es.amazonaws.com")
                .multiThreaded(true)
                .build());
        JestClient client = factory.getObject();

        // Create new index
        client.execute(new CreateIndex.Builder("amazon_price").build());

        // Run task every 10 minutes
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    // Load URL
                    Document document =
                            Jsoup.connect("https://www.amazon.com/NVIDIA-GEFORCE-GTX-1080-Founders/dp/B01GATSH9U/ref=sr_1_5?s=pc&ie=UTF8&qid=1489554837&sr=1-5&keywords=gtx+1080")
                                    .timeout(30000).userAgent("Mozilla/17.0").get();

                    // Get price
                    Element element = document.select("span[id=priceblock_ourprice]").first();

                    // Convert to double
                    double price = Double.parseDouble(element.text().replace("$", ""));

                    // Create new product
                    Product product = new Product(price, System.currentTimeMillis());

                    // Log
                    System.out.println("Update: " + product.price);
                    System.out.println("Time: " + product.timestamp);

                    // Send to AWS
                    Index index = new Index.Builder(product).index("amazon_price").type("item").build();
                    client.execute(index);

                } catch (Exception e) {
                }
            }
        }, 0, 10, TimeUnit.MINUTES);
    }

    static class Product {
        public double price;
        public long timestamp;

        public Product(double price, long timestamp) {
            this.price = price;
            this.timestamp = timestamp;
        }
    }
}
