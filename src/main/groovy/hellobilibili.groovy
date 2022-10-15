import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.sql.Sql
import io.vertx.core.Vertx
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.client.WebClientOptions
import org.postgresql.util.PGobject

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

def vertx = Vertx.vertx()

WebClientOptions options = new WebClientOptions()
  .setUserAgent("My-App/1.2.3");
options.setKeepAlive(false);

WebClient client = WebClient.create(vertx, options);

Sql db = Sql.newInstance("jdbc:postgresql://localhost:5432/postgres","postgres","123456","org.postgresql.Driver")

4881.times {
  def index = it+1
  client.getAbs("https://jobs.bilibili.com/api/srs/position/detail/$index")
    .putHeader("x-csrf",'c164a828-5376-442d-af3c-90a04082fdc8')
    .putHeader("x-usertype",'2')
    .send({ar ->
      if(ar.succeeded()){
        String _jsonString = ar.result().bodyAsString()

        def map = new JsonSlurper().parseText(_jsonString)

        def data = map.data

        data.id = data.id.toInteger()
        data.pushTime = LocalDateTime.parse(data.pushTime, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"))

        PGobject jsonObject = new PGobject()
        jsonObject.setType("json")
        jsonObject.setValue(JsonOutput.toJson(data))

        println data.id

        db.executeInsert("""
insert into bilibilijob(id, position_name, position_type_name, position_code_name, work_location, push_time, position_description, position_json)
values (?.id, ?.positionName, ?.positionTypeName, ?.positionCodeMame, ?.workLocation, ?.pushTime, ?.positionDescription, ?.positionJson)
on conflict (id) do update set
                                position_name = ?.positionName ,
                                position_type_name = ?.positionTypeName,
                                position_code_name = ?.positionCodeMame,
                                work_location = ?.workLocation,
                                push_time = ?.pushTime,
                                position_description = ?.positionDescription,
                                position_json = ?.positionJson
 """,
          map.data)
      }
    })
}


