package com.mongodb.quickstart;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.mongodb.client.model.Updates.set;

public class Connection {
    public static void main(String[] args) {
        final String connString = "mongodb+srv://twitch:umruJuosakcIxqVg@twitchcluster.tbsnotd.mongodb.net/?retryWrites=true&w=majority;";
        connectToDatabase(connString);
    }

    private static void connectToDatabase(String connString) {
        try (MongoClient mongoClient = MongoClients.create(connString)) {
            MongoCollection<Document> cookies = mongoClient.getDatabase("christmas").getCollection("cookies");

            // crud operations
            deleteDocuments(cookies);
            createDocuments(cookies);
            updateDocuments(cookies);
            readDocuments(cookies);

        }
    }

    private static void createDocuments(MongoCollection<Document> cookies) {
        List<Document> cookieList = new ArrayList<>();
        List<String> ingredients = List.of("flour", "butter", "milk", "eggs", "sugar", "read food coloring");
        for (int i = 0; i < 10; i++) {
            cookieList.add(new Document("cookie_id", i).append("color", "pink").append("ingredients", ingredients));
        }
        cookies.insertMany(cookieList);
    }

    private static void deleteDocuments(MongoCollection<Document> cookies) {
        cookies.deleteMany(Filters.in("color", "pink"));
    }

    private static void updateDocuments(MongoCollection<Document> cookies) {
//        cookies.updateMany(new Document(), unset("calories"));
        List<Document> docs = cookies.find().into(new ArrayList<>());
        docs.forEach(document -> cookies.updateOne(document, set("kcal", new Random().nextInt(1000))));

    }

    private static void readDocuments(MongoCollection<Document> cookies) {
        List<Document> docs = cookies.find().into(new ArrayList<>());
        docs.forEach(document -> System.out.println(document.toJson()));
    }
}
