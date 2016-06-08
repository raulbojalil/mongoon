#include <bson.h>
#include <bcon.h>
#include <mongoc.h>

#include "mongoose.h"

#define JSON(...) #__VA_ARGS__
		
#define MAX_COLLECTIONNAME_LENGTH 64
#define MAX_KEY_LENGTH 256
#define MAX_VALUE_LENGTH 8192

#define MAX_FIND_RESULTS 25

#define HTTP_BAD_REQUEST "HTTP/1.0 400 Bad Request\r\nContent-Length: 0\r\n\r\n"
#define HTTP_CHUNKED_ENCODING "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Type: application/json; charset=utf-8\r\n\r\n"
#define HTTP_NOT_FOUND "HTTP/1.0 404 Not Found\r\nContent-Type: application/json\r\nContent-Length: 45\r\n\r\n" JSON({"s":0,"err":"No se encontraron resultados."})

//mongoc_client_t *client;
mongoc_client_pool_t *pool;

static const char *s_http_port = "8000";
static const char *db_name = "";
static struct mg_serve_http_opts s_http_server_opts;

static const struct mg_str s_get_method = MG_STR("GET");
static const struct mg_str s_put_method = MG_STR("PUT");
static const struct mg_str s_delele_method = MG_STR("DELETE");
static const struct mg_str s_post_method = MG_STR("POST");


static void send_error_response(struct mg_connection *nc, bson_error_t* error)
{
	mg_printf(nc, "HTTP/1.0 500 Internal Server Error\r\n"
		"Content-Type: text/plain\r\n"
		"Content-Length: %d\r\n\r\n%s", strlen(error->message), error->message);
}

static void get_data_from_uri(struct mg_str *uri, struct mg_str *collection, struct mg_str *documentId)
{
	size_t pos = 0;
	collection->len = 0;
	documentId->len = 0;

	if (uri->len <= 1) return 0;

	if (uri->len > 13 && memcmp(uri->p, "/collections/", 13) == 0)
	{
		pos += 13;
		collection->p = uri->p + pos;
		int len = 0;
		while (*(uri->p + pos) && pos < uri->len)
		{
			if (*(uri->p + pos) == '/') break;
			pos++; len++;
		}

		collection->len = len;

		if (uri->len - pos > 11 && memcmp(uri->p + pos, "/documents/", 11) == 0)
		{
			pos += 11;
			documentId->p = uri->p + pos;
			len = 0;
			while (*(uri->p + pos) && pos < uri->len)
			{
				if (*(uri->p + pos) == '/') break;
				pos++; len++;
			}

			documentId->len = len;
		}
	}


}


// GET /collections/{collection name}/documents
static void handle_find_documents(struct mg_connection *nc, struct http_message *hm, struct mg_str* collectionNameMgStr)
{
	mongoc_client_t *client = mongoc_client_pool_pop(pool);
	mongoc_collection_t *collection;
	mongoc_cursor_t *cursor;
	bson_t *query;
	
	char* sortField;
	int sortFieldLen = -1;
	int sortOrder = 1;

	char collectionName[MAX_COLLECTIONNAME_LENGTH];
	memcpy(collectionName, collectionNameMgStr->p, collectionNameMgStr->len >= MAX_COLLECTIONNAME_LENGTH ? MAX_COLLECTIONNAME_LENGTH : collectionNameMgStr->len);
	collectionName[collectionNameMgStr->len >= MAX_COLLECTIONNAME_LENGTH ? MAX_COLLECTIONNAME_LENGTH - 1 : collectionNameMgStr->len] = '\0';

	query = bson_new();

	if (hm->query_string.len > 0)
	{
		char paramBuffer[MAX_KEY_LENGTH];
		char valueBuffer[MAX_VALUE_LENGTH];

		size_t query_string_pos = 0;
		char isKey = 1;
		char* i = hm->query_string.p;
		char* currentKey = hm->query_string.p;
		char* currentValue = 0;
		int currentKeyLen = 0;
		int currentValueLen = 0;

		while (*i && query_string_pos < hm->query_string.len)
		{
			if (isKey) currentKeyLen++; else currentValueLen++;

			if (*i == '=')
			{
				currentKeyLen--;
				currentValue = i + 1;
				currentValueLen = 0;
				isKey = 0;
			}

			if (*i == '&')
			{
				currentValueLen--;

				if (memcmp(currentKey, "_sortOrder", 10) == 0)
				{
					if (memcmp(currentValue, "DESC", currentValueLen) == 0)
						sortOrder = -1;
				}
				else if (memcmp(currentKey, "_sortField", 10) == 0)
				{
					sortField = currentValue;
					sortFieldLen = currentValueLen;
				}
				else
				{
					mg_url_decode(currentKey, currentKeyLen, paramBuffer, MAX_KEY_LENGTH, 1);
					mg_url_decode(currentValue, currentValueLen, valueBuffer, MAX_VALUE_LENGTH, 1);
					//bson_append_utf8(query, currentKey, currentKeyLen, currentValue, currentValueLen);
					bson_append_utf8(query, paramBuffer, strlen(paramBuffer), valueBuffer, strlen(valueBuffer));
				}

				currentKey = i + 1;
				currentKeyLen = 0;
				isKey = 1;

			}

			query_string_pos++;
			i++;
		}

		if (memcmp(currentKey, "_sortOrder", 10) == 0)
		{
			if (memcmp(currentValue, "DESC", currentValueLen) == 0)
				sortOrder = -1;
		}
		else if (memcmp(currentKey, "_sortField", 10) == 0)
		{
			sortField = currentValue;
			sortFieldLen = currentValueLen;
		}
		else
		{
			mg_url_decode(currentKey, currentKeyLen, paramBuffer, MAX_KEY_LENGTH, 1);
			mg_url_decode(currentValue, currentValueLen, valueBuffer, MAX_VALUE_LENGTH, 1);
			//bson_append_utf8(query, currentKey, currentKeyLen, currentValue, currentValueLen);
			bson_append_utf8(query, paramBuffer, strlen(paramBuffer), valueBuffer, strlen(valueBuffer));
		}
	}


	collection = mongoc_client_get_collection(client, db_name, collectionName);

	if (sortFieldLen > 0)
	{
		bson_t *order = bson_new();
		bson_append_int32(order, sortField, sortFieldLen, sortOrder);
		bson_t *queryWithSort = BCON_NEW("$query", BCON_DOCUMENT(query), "$orderby", BCON_DOCUMENT(order));
		cursor = mongoc_collection_find(collection, MONGOC_QUERY_NONE, 0, MAX_FIND_RESULTS, 0, queryWithSort, NULL, NULL);
		bson_destroy(queryWithSort);
		bson_destroy(order);
	}
	else cursor = mongoc_collection_find(collection, MONGOC_QUERY_NONE, 0, MAX_FIND_RESULTS, 0, query, NULL, NULL);
	

	const bson_t *doc;
	char* str;
	char chunked = 0;

	if (mongoc_cursor_next(cursor, &doc))
	{
		str = bson_as_json(doc, NULL);
		mg_printf(nc, "%s", HTTP_CHUNKED_ENCODING);
		mg_printf_http_chunk(nc, "[");
		str = bson_as_json(doc, NULL);
		mg_printf_http_chunk(nc, str);
		bson_free(str);
		bson_destroy(doc);
		chunked = 1;
	}
	else
		mg_printf(nc, HTTP_NOT_FOUND);
	
	while (mongoc_cursor_next(cursor, &doc)) {
		str = bson_as_json(doc, NULL);
		mg_printf_http_chunk(nc, ",%s", str);
		bson_free(str);
		bson_destroy(doc);
	}

	if (chunked)
	{
		mg_printf_http_chunk(nc, "]");
		mg_send_http_chunk(nc, "", 0);
	}

	bson_destroy(query);
	
	mongoc_collection_destroy(collection);
	mongoc_cursor_destroy(cursor);
	mongoc_client_pool_push(pool, client);

}

// PUT /collections/{collection name}/documents/{documentId}
static void handle_update_document(struct mg_connection *nc, struct http_message *hm, struct mg_str* collectionNameMgStr, struct mg_str* documentId)
{
	mongoc_client_t *client = mongoc_client_pool_pop(pool);
	mongoc_collection_t *collection;
	bson_t *query;
	bson_t *update;
	bson_error_t error; //Este no se necesita destruir

	char collectionName[MAX_COLLECTIONNAME_LENGTH];
	memcpy(collectionName, collectionNameMgStr->p, collectionNameMgStr->len >= MAX_COLLECTIONNAME_LENGTH ? MAX_COLLECTIONNAME_LENGTH : collectionNameMgStr->len);
	collectionName[collectionNameMgStr->len >= MAX_COLLECTIONNAME_LENGTH ? MAX_COLLECTIONNAME_LENGTH - 1 : collectionNameMgStr->len] = '\0';

	query = bson_new();
	bson_append_utf8(query, "_id", 3, documentId->p, documentId->len);

	update = bson_new();
	bson_t * update_set = bson_new();

	bson_append_document_begin(update, "$set", 4, update_set);
	
	if (hm->body.len > 0)
	{
		char paramBuffer[MAX_KEY_LENGTH];
		char valueBuffer[MAX_VALUE_LENGTH];

		size_t query_string_pos = 0;
		char isKey = 1;
		char* i = hm->body.p;
		char* currentKey = hm->body.p;
		char* currentValue = 0;
		int currentKeyLen = 0;
		int currentValueLen = 0;

		while (*i && query_string_pos < hm->body.len)
		{
			if (isKey) currentKeyLen++; else currentValueLen++;

			if (*i == '=')
			{
				currentKeyLen--;
				currentValue = i + 1;
				currentValueLen = 0;
				isKey = 0;
			}

			if (*i == '&')
			{
				currentValueLen--;

				if (*currentKey != "_")
				{
					mg_url_decode(currentKey, currentKeyLen, paramBuffer, MAX_KEY_LENGTH, 1);
					mg_url_decode(currentValue, currentValueLen, valueBuffer, MAX_VALUE_LENGTH, 1);
					//bson_append_utf8(update_set, currentKey, currentKeyLen, currentValue, currentValueLen);
					bson_append_utf8(update_set, paramBuffer, strlen(paramBuffer), valueBuffer, strlen(valueBuffer));
				}


				currentKey = i + 1;
				currentKeyLen = 0;
				isKey = 1;
			}

			query_string_pos++;
			i++;
		}

		if (*currentKey != "_")
		{
			mg_url_decode(currentKey, currentKeyLen, paramBuffer, MAX_KEY_LENGTH, 1);
			mg_url_decode(currentValue, currentValueLen, valueBuffer, MAX_VALUE_LENGTH, 1);
			//bson_append_utf8(update_set, currentKey, currentKeyLen, currentValue, currentValueLen);
			bson_append_utf8(update_set, paramBuffer, strlen(paramBuffer), valueBuffer, strlen(valueBuffer));
		}
	}

	bson_append_document_end(update, update_set);

	collection = mongoc_client_get_collection(client, db_name, collectionName);

	if (mongoc_collection_update(collection, MONGOC_UPDATE_NONE, query, update, NULL, &error))
		mg_printf(nc, "HTTP/1.0 200 OK\r\nContent-Type: application/json\r\nContent-Length: 16\r\n\r\n"
		JSON({"s":1,"err":""}));
	else
		send_error_response(nc, &error);

	bson_destroy(update_set);
	bson_destroy(update);
	bson_destroy(query);
	mongoc_collection_destroy(collection);
	mongoc_client_pool_push(pool, client);
}

// GET /collections/{collection name}/documents/{documentId}
static void handle_find_document(struct mg_connection *nc, struct http_message *hm, struct mg_str* collectionNameMgStr, struct mg_str* documentId)
{
	mongoc_client_t *client = mongoc_client_pool_pop(pool);
	mongoc_collection_t *collection;
	mongoc_cursor_t *cursor;
	bson_t *query;
	
	char collectionName[MAX_COLLECTIONNAME_LENGTH];
	memcpy(collectionName, collectionNameMgStr->p, collectionNameMgStr->len >= MAX_COLLECTIONNAME_LENGTH ? MAX_COLLECTIONNAME_LENGTH : collectionNameMgStr->len);
	collectionName[collectionNameMgStr->len >= MAX_COLLECTIONNAME_LENGTH ? MAX_COLLECTIONNAME_LENGTH - 1 : collectionNameMgStr->len] = '\0';

	query = bson_new();
	bson_append_utf8(query, "_id", 3, documentId->p, documentId->len);
	
	collection = mongoc_client_get_collection(client, db_name, collectionName);
	//mongoc_read_prefs_t * readPrefs = mongoc_read_prefs_new(MONGOC_READ_SECONDARY_PREFERRED);
	cursor = mongoc_collection_find(collection, MONGOC_QUERY_NONE, 0, 0, 0, query, NULL, NULL);

	const bson_t *doc;
	char* str;

	if (mongoc_cursor_next(cursor, &doc))
	{
		size_t str_length;
		str = bson_as_json(doc, &str_length);
		
		mg_printf(nc, "HTTP/1.0 200 OK\r\n"
			"Content-Type: application/json\r\n"
			"Content-Length: %d\r\n\r\n%s", str_length, str);

		bson_free(str);
		bson_destroy(doc);
	}
	else
		mg_printf(nc, HTTP_NOT_FOUND);

	//mongoc_read_prefs_destroy(readPrefs);
	bson_destroy(query);
	mongoc_collection_destroy(collection);
	mongoc_cursor_destroy(cursor);
	mongoc_client_pool_push(pool, client);
}

// DELETE /collections/{collection name}/documents/{documentId}
static void handle_delete_document(struct mg_connection *nc, struct http_message *hm, struct mg_str* collectionNameMgStr, struct mg_str* documentId)
{
	mongoc_client_t *client = mongoc_client_pool_pop(pool);
	mongoc_collection_t *collection;
	bson_t *query;
	bson_error_t error; //Este no se necesita destruir

	char collectionName[MAX_COLLECTIONNAME_LENGTH];
	memcpy(collectionName, collectionNameMgStr->p, collectionNameMgStr->len >= MAX_COLLECTIONNAME_LENGTH ? MAX_COLLECTIONNAME_LENGTH : collectionNameMgStr->len);
	collectionName[collectionNameMgStr->len >= MAX_COLLECTIONNAME_LENGTH ? MAX_COLLECTIONNAME_LENGTH - 1 : collectionNameMgStr->len] = '\0';

	query = bson_new();
	bson_append_utf8(query, "_id", 3, documentId->p, documentId->len);

	collection = mongoc_client_get_collection(client, db_name, collectionName);

    if (mongoc_collection_remove(collection, MONGOC_REMOVE_SINGLE_REMOVE, query, NULL, &error))
		mg_printf(nc, "HTTP/1.0 200 OK\r\nContent-Type: application/json\r\nContent-Length: 16\r\n\r\n"
		JSON({"s":1,"err":""}));
	else
		send_error_response(nc, &error);

	bson_destroy(query);
	mongoc_collection_destroy(collection);
	mongoc_client_pool_push(pool, client);
}

// POST /collections/{collection name}/documents/{documentId}
static void handle_add_document(struct mg_connection *nc, struct http_message *hm, struct mg_str* collectionNameMgStr, struct mg_str* documentId)
{
	mongoc_client_t *client = mongoc_client_pool_pop(pool);
	mongoc_collection_t *collection;
	bson_t *document;
	bson_error_t error; //Este no se necesita destruir

	char collectionName[MAX_COLLECTIONNAME_LENGTH];
	memcpy(collectionName, collectionNameMgStr->p, collectionNameMgStr->len >= MAX_COLLECTIONNAME_LENGTH ? MAX_COLLECTIONNAME_LENGTH : collectionNameMgStr->len);
	collectionName[collectionNameMgStr->len >= MAX_COLLECTIONNAME_LENGTH ? MAX_COLLECTIONNAME_LENGTH - 1 : collectionNameMgStr->len] = '\0';

	document = bson_new();
	
	//bson_t* emptyArray = bson_new();

	if (hm->body.len > 0)
	{
		char paramBuffer[MAX_KEY_LENGTH];
		char valueBuffer[MAX_VALUE_LENGTH];

		size_t query_string_pos = 0;
		char isKey = 1;
		char* i = hm->body.p;
		char* currentKey = hm->body.p;
		char* currentValue = 0;
		int currentKeyLen = 0;
		int currentValueLen = 0;

		while (*i && query_string_pos < hm->body.len)
		{
			if (isKey) currentKeyLen++; else currentValueLen++;

			if (*i == '=')
			{
				currentKeyLen--;
				currentValue = i + 1;
				currentValueLen = 0;
				isKey = 0;
			}

			if (*i == '&')
			{
				currentValueLen--;

				mg_url_decode(currentKey, currentKeyLen, paramBuffer, MAX_KEY_LENGTH, 1);
				mg_url_decode(currentValue, currentValueLen, valueBuffer, MAX_VALUE_LENGTH, 1);
				//bson_append_utf8(document, currentKey, currentKeyLen, currentValue, currentValueLen);
				bson_append_utf8(document, paramBuffer, strlen(paramBuffer), valueBuffer, strlen(valueBuffer));

				currentKey = i + 1;
				currentKeyLen = 0;
				isKey = 1;
			}

			query_string_pos++;
			i++;
		}

		mg_url_decode(currentKey, currentKeyLen, paramBuffer, MAX_KEY_LENGTH, 1);
		mg_url_decode(currentValue, currentValueLen, valueBuffer, MAX_VALUE_LENGTH, 1);
		//bson_append_utf8(document, currentKey, currentKeyLen, currentValue, currentValueLen);
		bson_append_utf8(document, paramBuffer, strlen(paramBuffer), valueBuffer, strlen(valueBuffer));
	}


	bson_append_utf8(document, "_id", 3, documentId->p, documentId->len);

	collection = mongoc_client_get_collection(client, db_name, collectionName);

	if (mongoc_collection_insert(collection, MONGOC_INSERT_NONE, document, NULL, &error))
	{
		size_t str_length;
		char* str = bson_as_json(document, &str_length);
		mg_printf(nc, "HTTP/1.0 200 OK\r\n"
			"Content-Type: application/json\r\n"
			"Content-Length: %d\r\n\r\n%s", str_length, str);
		bson_free(str);
	}
	else
		send_error_response(nc, &error);

	bson_destroy(document);
	//bson_destroy(emptyArray);
	mongoc_collection_destroy(collection);
	mongoc_client_pool_push(pool, client);
}

static int is_equal(const struct mg_str *s1, const struct mg_str *s2) {
	return s1->len == s2->len && memcmp(s1->p, s2->p, s2->len) == 0;
}

static void ev_handler(struct mg_connection *nc, int ev, void *ev_data) {

	struct http_message *hm = (struct http_message *) ev_data;

	switch (ev) {
	case MG_EV_HTTP_REQUEST:
	{
		struct mg_str collection;
		struct mg_str documentId;
		get_data_from_uri(&hm->uri, &collection, &documentId);

		if (collection.len == 0 && documentId.len == 0)
			mg_printf(nc, HTTP_BAD_REQUEST);
		else
		{   
			if (is_equal(&hm->method, &s_get_method)) {

				if (documentId.len > 0)
					handle_find_document(nc, hm, &collection, &documentId);
				else
					handle_find_documents(nc, hm, &collection);
			}
			else if (is_equal(&hm->method, &s_post_method))
			{
				if (documentId.len == 0)
					mg_printf(nc, HTTP_BAD_REQUEST);
				else
					handle_add_document(nc, hm, &collection, &documentId);
			}
			else if (is_equal(&hm->method, &s_put_method))
			{
				if (documentId.len == 0)
					mg_printf(nc, HTTP_BAD_REQUEST);
				else
					handle_update_document(nc, hm, &collection, &documentId);
			}
			else if (is_equal(&hm->method, &s_delele_method))
			{
				if (documentId.len == 0)
					mg_printf(nc, HTTP_BAD_REQUEST);
				else
					handle_delete_document(nc, hm, &collection, &documentId);
			}
			else
				mg_printf(nc, HTTP_BAD_REQUEST);
		}

		break;
	}
	default:
		break;
	}
}

int main(int argc, char *argv[]){

	if (argc < 4) {
		printf("MongoDB REST Server by Raúl Bojalil.\n\nUsage: MongoRESTServer [MONGODB_CONN_STRING] [DB_NAME] [SERVER_PORT]\n\n");
		return;
	}

	db_name = argv[2];
	s_http_port = argv[3];

	struct mg_mgr mgr;
	struct mg_connection *nc;

	mongoc_init();

	//client = mongoc_client_new(MONGO_CONN_STRING);
	mongoc_uri_t* uri = mongoc_uri_new(argv[1]);
	
	pool = mongoc_client_pool_new(uri);

	mg_mgr_init(&mgr, NULL);
	nc = mg_bind(&mgr, s_http_port, ev_handler);

	mg_set_protocol_http_websocket(nc);
	mg_enable_multithreading(nc);

	printf("Server running on port %s...\n", s_http_port);
	for (;;) {
		mg_mgr_poll(&mgr, 1000);
	}

	mg_mgr_free(&mgr);

	mongoc_client_pool_destroy(pool);
	mongoc_uri_destroy(uri);
	//mongoc_client_destroy(client);
	mongoc_cleanup();

	return;

}