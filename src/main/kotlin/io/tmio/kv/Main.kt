package io.tmio.kv

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.web.bind.annotation.*
import java.util.*


@RestController
class RestAPI {

    val om = ObjectMapper()

    val store = HashMap<String,Any>()

    @Autowired
    lateinit var index : Index

    @RequestMapping("/rest/kv/{id}", method = arrayOf(RequestMethod.GET))
    fun get(@PathVariable("id") id: String) : Any? {
        return 
    }

    @RequestMapping("/rest/kv/{id}", method = arrayOf(RequestMethod.PUT))
    fun put(@PathVariable("id") id: String, @RequestBody json : String) {
        val node = om.readTree(json)
        store.put(id, node)
        index.index(id, node)
    }

    @RequestMapping("/rest/kv/{id}", method = arrayOf(RequestMethod.DELETE))
    fun delete(@PathVariable("id") id: String) {
        store.remove(id)
        index.remove(id)
    }

    @RequestMapping("/rest/search", method = arrayOf(RequestMethod.GET))
    fun search(@RequestParam("q") q: String) : List<Any?> {
        return index.search(q).map { id -> store.get(id) }
    }
}

@SpringBootApplication
open class Application {

}

fun main(args: Array<String>) {
    SpringApplication.run(Application::class.java, *args)
}