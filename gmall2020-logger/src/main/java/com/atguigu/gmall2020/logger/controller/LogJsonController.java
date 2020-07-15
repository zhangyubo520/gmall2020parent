package com.atguigu.gmall2020.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;



@RestController
public class LogJsonController {


//    public class Demo1Controller {
//        @ResponseBody
//
//    }

    @Autowired
    KafkaTemplate kafkaTemplate;

    @RequestMapping("/applog")
    public String applog(@RequestBody String json){

        System.out.println(json);
        JSONObject jsonObject = JSON.parseObject(json);
        if (jsonObject.getString("start") != null && jsonObject.getString("start").length() > 0) {
            kafkaTemplate.send("GMALL_START", json);
        } else {
            kafkaTemplate.send("GMALL_EVENT", json);
        }
        return "hello demo";

    }

}
