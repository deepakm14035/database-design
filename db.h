/********************************************************************
db.h - This file contains all the structures, defines, and function
	prototype for the db.exe program.
*********************************************************************/
#pragma once
#include <string.h>
#include <stdio.h>

#define MAX_IDENT_LEN   16
#define MAX_NUM_COL			16
#define MAX_TOK_LEN			32
#define KEYWORD_OFFSET	10
#define STRING_BREAK		" (),<>="
#define NUMBER_BREAK		" ),"

/* Column descriptor sturcture = 20+4+4+4+4 = 36 bytes */
typedef struct cd_entry_def
{
	char		col_name[MAX_IDENT_LEN+4];
	int			col_id;                   /* Start from 0 */
	int			col_type;
	int			col_len;
	int 		not_null;
} cd_entry;

/* Table packed descriptor sturcture = 4+20+4+4+4 = 36 bytes
   Minimum of 1 column in a table - therefore minimum size of
	 1 valid tpd_entry is 36+36 = 72 bytes. */
typedef struct tpd_entry_def
{
	int				tpd_size;
	char			table_name[MAX_IDENT_LEN+4];
	int				num_columns;
	int				cd_offset;
	int       tpd_flags;
} tpd_entry;

/* Table packed descriptor list = 4+4+4+36 = 48 bytes.  When no
   table is defined the tpd_list is 48 bytes.  When there is 
	 at least 1 table, then the tpd_entry (36 bytes) will be
	 overlapped by the first valid tpd_entry. */
typedef struct tpd_list_def
{
	int				list_size;
	int				num_tables;
	int				db_flags;
	tpd_entry	tpd_start;
}tpd_list;

/* This token_list definition is used for breaking the command
   string into separate tokens in function get_tokens().  For
	 each token, a new token_list will be allocated and linked 
	 together. */
typedef struct t_list
{
	char	tok_string[MAX_TOK_LEN];
	int		tok_class;
	int		tok_value;
	struct t_list *next;
} token_list;

/* This enum defines the different classes of tokens for 
	 semantic processing. */
typedef enum t_class
{
	keyword = 1,	// 1
	identifier,		// 2
	symbol, 			// 3
	type_name,		// 4
	constant,		  // 5
  function_name,// 6
	terminator,		// 7
	error			    // 8
  
} token_class;

/* This enum defines the different values associated with
   a single valid token.  Use for semantic processing. */
typedef enum t_value
{
	T_INT = 10,		// 10 - new type should be added above this line
	T_CHAR,		    // 11 
	T_VARCHAR,		    // 12       
	K_CREATE, 		// 13
	K_TABLE,			// 14
	K_NOT,				// 15
	K_NULL,				// 16
	K_DROP,				// 17
	K_LIST,				// 18
	K_SCHEMA,			// 19
	K_FOR,        // 20
	K_TO,				  // 21
	K_INSERT,     // 22
	K_INTO,       // 23
	K_VALUES,     // 24
	K_DELETE,     // 25
	K_FROM,       // 26
	K_WHERE,      // 27
	K_UPDATE,     // 28
	K_SET,        // 29
	K_SELECT,     // 30
	K_ORDER,      // 31
	K_GROUP,	  // 32
	K_BY,         // 33
	K_DESC,       // 34
	K_IS,         // 35
	K_AND,        // 36
	K_NATURAL,    // 37
	K_JOIN,       // 38
	K_OR,         // 39 - new keyword should be added below this line
	F_SUM,        // 40
	F_AVG,        // 41
	F_COUNT,      // 42 - new function name should be added below this line
	S_LEFT_PAREN = 70,  // 70
	S_RIGHT_PAREN,		  // 71
	S_COMMA,			      // 72
	S_STAR,             // 73
	S_EQUAL,            // 74
	S_LESS,             // 75
	S_GREATER,          // 76
	IDENT = 85,			    // 85
	INT_LITERAL = 90,	  // 90
	STRING_LITERAL,     // 91
	EOC = 95,			      // 95
	INVALID = 99		    // 99
} token_value;

/* This constants must be updated when add new keywords */
#define TOTAL_KEYWORDS_PLUS_TYPE_NAMES 32

/* New keyword must be added in the same position/order as the enum
   definition above, otherwise the lookup will be wrong */
const char *keyword_table[] = 
{
  "int", "char", "varchar", "create", "table", "not", "null", "drop", "list", "schema",
  "for", "to", "insert", "into", "values", "delete", "from", "where", 
  "update", "set", "select", "order", "group", "by", "desc", "is", "and", "natural", "join", "or", 
  "sum", "avg", "count"
};

/* This enum defines a set of possible statements */
typedef enum s_statement
{
  INVALID_STATEMENT = -199,	// -199
	CREATE_TABLE = 100,				// 100
	DROP_TABLE,								// 101
	LIST_TABLE,								// 102
	LIST_SCHEMA,							// 103
  INSERT,                   // 104
  DELETE,                   // 105
  UPDATE,                   // 106
  SELECT                    // 107
} semantic_statement;

/* This enum has a list of all the errors that should be detected
   by the program.  Can append to this if necessary. */
typedef enum error_return_codes
{
	INVALID_TABLE_NAME = -399,		// -399
	DUPLICATE_TABLE_NAME,			// -398
	TABLE_NOT_EXIST,				// -397
	INVALID_TABLE_DEFINITION,		// -396
	INVALID_COLUMN_NAME,			// -395
	DUPLICATE_COLUMN_NAME,			// -394
	COLUMN_NOT_EXIST,				// -393
	MAX_COLUMN_EXCEEDED,			// -392
	INVALID_TYPE_NAME,				// -391
	INVALID_COLUMN_DEFINITION,		// -390
	INVALID_COLUMN_LENGTH,			// -389
  INVALID_REPORT_FILE_NAME,			// -388
  /* Must add all the possible errors from I/U/D + SELECT here */
	INVALID_SYNTAX,					// -387
	INVALID_INSERT_DEFINITION,		// -386
	INVALID_INSERT_STATEMENT,		// -385
	FILE_OPEN_ERROR = -299,			// -299
	DBFILE_CORRUPTION,				// -298
	MEMORY_ERROR,					// -297
	INVALID_SELECT_SECTION,			// -296
	NULL_NOT_ALLOWED,				// -295
	MAX_LENGTH_EXCEEDED,			// -294
	INCOMPLETE_INSERT_STATEMENT,	// -293
	INVALID_UPDATE_STATEMENT,		// -292
	DATA_TYPE_MISMATCH,				// -291
	INVALID_AGGREGATE_FUNCTION,		// -290
	NO_MATCHING_COLUMNS				// -289

} return_codes;

typedef struct table_file_header_def
{
	int			file_size;			// 4 bytes
	int			record_size;			// 4 bytes
	int			num_records;			// 4 bytes
	int			record_offset;			// 4 bytes
	int			file_header_flag;		// 4 bytes
	tpd_entry		*tpd_ptr;			// 4 bytes
} table_file_header;

typedef enum condition_type_def
{
  IS_NOT_NULL = 400,		//400
  IS_NULL,					//402 
  EQUALS,					//403 
  NOT_EQUALS,				//404
  GREATER_THAN,				//405 
  LESS_THAN,				//406 
  INVALID_CONDITION			//407
} condition_type;

typedef enum aggregate_type_def
{
  NONE = 500,		//500
  AVG,				//501
  SUM,				//502
  COUNT,			//503
} aggregate_type;


struct condition{
	int colNo;
	condition_type type;
	int keywordLen;
	int nextBinaryOperator;
	char data[32];
};

struct select_attribute{
	char columnName[32];
	char tableName[32];
	char* concatenatedName;//containing aggregate function
	aggregate_type functionType;
	int columnIndex;
	int columnOffset;
	int columnLength;
	select_attribute** arr;
};

struct update_operation{
	char* columnName;
	char* newValue;
	int columnIndex;
	int columnType;
};

/* Set of function prototypes */
int 				get_token(char *command, token_list **tok_list);
void 				add_to_list(token_list **tok_list, const char *tmp, int t_class, int t_value);
int 				do_semantic(token_list *tok_list);
int 				sem_create_table(token_list *t_list);
int 				sem_drop_table(token_list *t_list);
int 				sem_list_tables();
int 				sem_list_schema(token_list *t_list);
int 				sem_delete_from(token_list *t_list);
void 				printCharArrInInt(char arr[], int size);
bool* 				filterRows(char** records, tpd_entry *tab_entry, condition* conditionList, int conditionCount, int rowCount, int rowLen);
int 				parseWhereClause(token_list* cur, tpd_entry* tab_entry, condition* conditionList);
condition* 			parseCondition(token_list *t_list, int colType);
int 				getRowOffset(tpd_entry *tab_entry, int colNo);
int 				bin2int(char* num);
char* 				toLower(char* s);
int 				stringToInt(char arr[]);
int 				getColumnList(token_list* cur, select_attribute** columnList);
int				 	filterColumns(select_attribute** attributes, int attributeCount, tpd_entry *tab_entry);
int 				checkAggregate(aggregate_type aggType, token_list* cur, select_attribute* attr);
int 				parseSetClause(token_list* cur, tpd_entry* tab_entry, update_operation** columnListToUpdate);
int 				sem_update(token_list *t_list);
int 				filterColumns_bk(select_attribute** attributes, int attributeCount, tpd_entry *tab_entry, select_attribute** filters);
void 				printDashes(select_attribute** columnsInSelect, tpd_entry* tab_entry, int columnCountInSelect);
void 				printColumnList(select_attribute** columnsInSelect, tpd_entry* tab_entry, int columnCountInSelect);
void 				printCells(select_attribute** columnsInSelect, int columnCountInSelect, int rowNo, tpd_entry* tab_entry, char** records);
void 				printRowData(select_attribute** columnsInSelect, int columnCountInSelect, int rowCount, tpd_entry* tab_entry, bool* rowsToPrint, char** records);
char** 				getTableData(tpd_entry* tab_entry, table_file_header* tfh);
int 				getJoinedData(tpd_entry* tab_entry1, tpd_entry* tab_entry2, char** records1, char** records2, table_file_header* tfh1, table_file_header* tfh2, select_attribute** columnsInSelect, int columnCount, char** outputRows);
/*
	Keep a global list of tpd - in real life, this will be stored
	in shared memory.  Build a set of functions/methods around this.
*/
tpd_list	*g_tpd_list;

int 				initialize_tpd_list();
int 				add_tpd_to_list(tpd_entry *tpd);
int 				drop_tpd_from_list(char *tabname);
tpd_entry* 			get_tpd_from_list(char *tabname);
int 				sem_insert_into(token_list *t_list);
void 				printCharArr(char arr[], int size);
void 				copyBytes(char* from, char to[], int noOfBytes);
int 				sem_select(token_list *t_list);
int 				columnExists(char* name, tpd_entry* tab_entry);
int 				getRowLen(select_attribute** columnList, int colLen);
void 				copyIntToCharArray(char* arr, int val);


struct row_obj{
	char* rowData;
	int offset;
	int dataType;
	bool shouldPrint=true;
	bool lessThan(row_obj& other){
		if(dataType==T_INT){
			return bin2int(rowData+offset+1) < bin2int(other.rowData+other.offset+1);
		}else{
			return strcmp(rowData+offset+1,other.rowData+other.offset+1)<0;
		}
	}
	bool equals(row_obj& other){
		if(dataType==T_INT){
			return bin2int(rowData+offset+1) == bin2int(other.rowData+other.offset+1);
		}else{
			return strcmp(rowData+offset+1,other.rowData+other.offset+1)==0;
		}
	}
};
