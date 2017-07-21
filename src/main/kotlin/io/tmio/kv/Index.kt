package io.tmio.kv

import com.fasterxml.jackson.databind.JsonNode
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.document.StringField
import org.apache.lucene.document.TextField
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.store.FSDirectory
import org.springframework.stereotype.Component
import java.io.IOException
import java.nio.file.Paths
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.IndexReader
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.Collector
import org.apache.lucene.search.TopDocsCollector
import javax.annotation.PostConstruct
import javax.annotation.PreDestroy


@Component
class Index {

    private val INDEX_DIR = "/tmp/lucene6idx"

    @Throws(IOException::class)
    private fun createWriter(): IndexWriter {
        val dir = FSDirectory.open(Paths.get(INDEX_DIR))
        val config = IndexWriterConfig(StandardAnalyzer())
        val writer = IndexWriter(dir, config)
        return writer
    }

    @Throws(IOException::class)
    private fun createSearcher(): IndexSearcher {
        val reader = DirectoryReader.open(writer)
        val searcher = IndexSearcher(reader)
        return searcher
    }

    val writer = createWriter()

    fun index(id: String, node: JsonNode) {
        remove(id)
        val doc = Document()
        doc.add(StringField("_id", id, Field.Store.YES))
        for (fieldName in node.fieldNames()) {
            doc.add(TextField(fieldName, node.get(fieldName).textValue(), Field.Store.NO))
            //doc.add(StringField(fieldName, node.get(fieldName).textValue(), Field.Store.NO))
        }
        writer.addDocument(doc)
    }

    fun remove(id: String) {
        val qp = QueryParser("_id", StandardAnalyzer())
        val q = qp.parse(id)
        writer.deleteDocuments(q)
    }

    fun search(query: String): List<String> {
        val qp = QueryParser("_id", StandardAnalyzer())
        val q = qp.parse(query)

        val searcher = createSearcher()
        val hits = searcher.search(q, 10)
        val results = hits.scoreDocs.map { sd -> searcher.doc(sd.doc).getField("_id").stringValue() }
        return results
    }

    @PreDestroy
    fun beforeDestroy() {
        writer.close()
    }
}

