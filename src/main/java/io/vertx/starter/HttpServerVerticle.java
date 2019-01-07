package io.vertx.starter;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpServerVerticle extends AbstractVerticle {

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpServerVerticle.class);
  public static final String CONFIG_HTTP_SERVER_PORT = "http.server.port";
  public static final String CONFIG_WIKIDB_QUEUE = "wikidb.queue";

  private String wikiDbQueue = "wikidb.queue";

  @Override
  public void start(Future<Void> startFuture) throws Exception {

    wikiDbQueue = config().getString(CONFIG_WIKIDB_QUEUE, "wikidb.queue");
    HttpServer server = vertx.createHttpServer();

    Router router = Router.router(vertx);
    router.get("/").handler(this::indexHandler);
    router.post().handler(BodyHandler.create());
    router.post("/save").handler(this::pageCreateHandler);

    int portNumber = config().getInteger(CONFIG_HTTP_SERVER_PORT, 8080);
    server
      .requestHandler(router)
      .listen(portNumber, ar -> {
        if (ar.succeeded()) {
          LOGGER.info("Http server started : " + portNumber);
          startFuture.complete();
        } else {
          startFuture.fail(ar.cause());
        }
      });
  }

  private void pageCreateHandler(RoutingContext routingContext) {

    JsonObject jsonObject = routingContext.getBodyAsJson();
    DeliveryOptions options = new DeliveryOptions().addHeader("actions", "create");

    vertx.eventBus().send(wikiDbQueue, jsonObject, options, reply -> {
      if (reply.succeeded()) {
        routingContext.response().end("Success");
      } else {
        LOGGER.error("Exception occurred :: " + reply.cause());
        routingContext.fail(reply.cause());
      }
    });
  }


  private void indexHandler(RoutingContext routingContext) {

    DeliveryOptions options = new DeliveryOptions().addHeader("action", "list");
    vertx.eventBus().send(wikiDbQueue, new JsonObject(), options, reply -> {

      if (reply.succeeded()) {
        JsonObject jsonObject = (JsonObject) reply.result().body();
        routingContext.response().end(jsonObject.getJsonArray("pages").getList().toString());
      } else {
        routingContext.fail(reply.cause());
      }
    });
  }
}
