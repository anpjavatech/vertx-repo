package io.vertx.starter;


import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.templ.freemarker.FreeMarkerTemplateEngine;

import java.util.List;
import java.util.stream.Collectors;

public class MainVerticle extends AbstractVerticle {

  private JDBCClient dbClient;
  private FreeMarkerTemplateEngine templateEngine;
  private static final Logger logger = LoggerFactory.getLogger(MainVerticle.class);

  private static final String SQL_CREATE_PAGES_TABLE = "create table if not exists Pages (Id integer identity primary key, Name varchar(255) unique, Content clob )";
  private static final String SQL_ALL_PAGES = "select Name from Pages";
  private static final String SQL_GET_PAGE = "select Id, Content from Pages where Name = ?";
  private static final String SQL_CREATE_PAGE = "insert into Pages values (NULL,?,?)";
  private static final String SQL_SAVE_PAGE = "update Pages set Content=? where Id =?";
  private static final String SQL_DELETE_PAGE = "delete from Pages where Id=?";



  @Override
  public void start(Future<Void> startFuture) throws Exception {

    Future<Void> steps = prepareDatabase().compose(v -> startHttpServer());
    steps.setHandler(startFuture.completer());
  }


  private Future<Void> prepareDatabase() {

    Future<Void> future = Future.future();
    dbClient = JDBCClient.createShared(vertx, new JsonObject()
      .put("url", "jdbc:hsqldb:file:db/wiki")
      .put("driver_class", "org.hsqldb.jdbcDriver")
      .put("max_pool_size", 30));

    dbClient.getConnection(han -> {
      if (han.failed()) {
        logger.error("Database connection got error..", han.cause());
      } else {
        SQLConnection connection = han.result();
        connection.execute(SQL_CREATE_PAGES_TABLE, create -> {
          connection.close();
          if (create.failed()) {
            logger.error("Database preparation error..", create.cause());
            future.fail(create.cause());
          } else {
            future.complete();
          }
        });
      }
    });

    return future;

  }


  private Future<Void> startHttpServer() {

    Future<Void> future = Future.future();
    HttpServer server = vertx.createHttpServer();
    Router router = Router.router(vertx);
    router.get("/").handler(this::indexHandler);
   // templateEngine = FreeMarkerTemplateEngine.create(vertx);

    server
      .requestHandler(router)
      .listen(8080, han ->{
      if(han.succeeded()){
       logger.info("Http Server listening to port 8080");
       future.complete();
      }else {
        logger.error("Could not start server", han.cause());
        future.fail(han.cause());
      }
    });

    return future;

  }

  private void indexHandler(RoutingContext routingContext) {

    dbClient.getConnection(han ->{
      if(han.succeeded()){
        SQLConnection connection = han.result();
        connection.query(SQL_ALL_PAGES, res->{
          connection.close();
          if(res.succeeded()){
            List<String> pages = res
                                    .result()
                                    .getResults()
                                    .stream()
                                    .map(json-> json.getString(0))
                                    .sorted()
                                    .collect(Collectors.toList());
            routingContext
              .response()
              .putHeader("Content-Type", "application/json")
              .end("string");

          }else {
            logger.error("Query Response error",res.cause());
            routingContext.fail(res.cause());
          }
        });
      }else {
        logger.error("Database Connection error", han.cause());
        routingContext.fail(han.cause());
      }
    });
  }
}
