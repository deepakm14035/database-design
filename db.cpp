/************************************************************
	Project#1:	CLP & DDL
 ************************************************************/

#include "db.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <algorithm>

#if defined(_WIN32) || defined(_WIN64)
#define strcasecmp _stricmp
#endif
#define IS_DEBUG false


void *__gxx_personality_v0;

int main(int argc, char** argv)
{
	int rc = 0;
	token_list *tok_list=NULL, *tok_ptr=NULL, *tmp_tok_ptr=NULL;
	//memcpy(argv[1],"\"insert into emp4 values ('deepak',25)\"", 40);
	if ((argc != 2) || (strlen(argv[1]) == 0))
	{
		printf("Usage: db \"command statement\"\n");
		return 1;
	}

	rc = initialize_tpd_list();

  if (rc)
  {
		printf("\nError in initialize_tpd_list().\nrc = %d\n", rc);
  }
	else
	{
    rc = get_token(argv[1], &tok_list);

		/* Test code */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
			printf("%16s \t%d \t %d\n",tok_ptr->tok_string, tok_ptr->tok_class,
				      tok_ptr->tok_value);
			tok_ptr = tok_ptr->next;
		}
    
		if (!rc)
		{
			rc = do_semantic(tok_list);
		}

		if (rc)
		{
			tok_ptr = tok_list;
			while (tok_ptr != NULL)
			{
				if ((tok_ptr->tok_class == error) ||
					  (tok_ptr->tok_value == INVALID))
				{
					printf("\nError in the string: %s\n", tok_ptr->tok_string);
					printf("rc=%d\n", rc);
					break;
				}
				tok_ptr = tok_ptr->next;
			}
		}

    /* Whether the token list is valid or not, we need to free the memory */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
      tmp_tok_ptr = tok_ptr->next;
      free(tok_ptr);
      tok_ptr=tmp_tok_ptr;
		}
	}

	return rc;
}

/************************************************************* 
	This is a lexical analyzer for simple SQL statements
 *************************************************************/
int get_token(char* command, token_list** tok_list)
{
	int rc=0,i,j;
	char *start, *cur, temp_string[MAX_TOK_LEN];
	bool done = false;
	
	start = cur = command;
	while (!done)
	{
		bool found_keyword = false;

		/* This is the TOP Level for each token */
	  memset ((void*)temp_string, '\0', MAX_TOK_LEN);
		i = 0;

		/* Get rid of all the leading blanks */
		while (*cur == ' ')
			cur++;

		if (cur && isalpha(*cur))
		{
			// find valid identifier
			int t_class;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((isalnum(*cur)) || (*cur == '_'));

			if (!(strchr(STRING_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				   is not a blank, (, ), or a comma, then append this
					 character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{

				// We have an identifier with at least 1 character
				// Now check if this ident is a keyword
				for (j = 0, found_keyword = false; j < TOTAL_KEYWORDS_PLUS_TYPE_NAMES; j++)
				{
					if ((strcasecmp(keyword_table[j], temp_string) == 0))
					{
						found_keyword = true;
						break;
					}
				}

				if (found_keyword)
				{
				  if (KEYWORD_OFFSET+j < K_CREATE)
						t_class = type_name;
					else if (KEYWORD_OFFSET+j >= F_SUM)
            			t_class = function_name;
          			else
					  t_class = keyword;

					add_to_list(tok_list, temp_string, t_class, KEYWORD_OFFSET+j);
				}
				else
				{
					if (strlen(temp_string) <= MAX_IDENT_LEN)
					  add_to_list(tok_list, temp_string, identifier, IDENT);
					else
					{
						add_to_list(tok_list, temp_string, error, INVALID);
						rc = INVALID;
						done = true;
					}
				}

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if (isdigit(*cur))
		{
			// find valid number
			do 
			{
				temp_string[i++] = *cur++;
			}
			while (isdigit(*cur));

			if (!(strchr(NUMBER_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				   is not a blank or a ), then append this
					 character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{
				add_to_list(tok_list, temp_string, constant, INT_LITERAL);

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if ((*cur == '(') || (*cur == ')') || (*cur == ',') || (*cur == '*')
		         || (*cur == '=') || (*cur == '<') || (*cur == '>'))
		{
			/* Catch all the symbols here. Note: no look ahead here. */
			int t_value;
			switch (*cur)
			{
				case '(' : t_value = S_LEFT_PAREN; break;
				case ')' : t_value = S_RIGHT_PAREN; break;
				case ',' : t_value = S_COMMA; break;
				case '*' : t_value = S_STAR; break;
				case '=' : t_value = S_EQUAL; break;
				case '<' : t_value = S_LESS; break;
				case '>' : t_value = S_GREATER; break;
			}

			temp_string[i++] = *cur++;

			add_to_list(tok_list, temp_string, symbol, t_value);

			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
		}
    else if (*cur == '\'')
    {
      /* Find STRING_LITERRAL */
			int t_class;
      cur++;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((*cur) && (*cur != '\''));

      temp_string[i] = '\0';

			if (!*cur)
			{
				/* If we reach the end of line */
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
      else /* must be a ' */
      {
        add_to_list(tok_list, temp_string, constant, STRING_LITERAL);
        cur++;
				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
        }
      }
    }
		else
		{
			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
			else
			{
				/* not a ident, number, or valid symbol */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
		}
	}
			
  return rc;
}

void add_to_list(token_list **tok_list, const char *tmp, int t_class, int t_value)
{
	token_list *cur = *tok_list;
	token_list *ptr = NULL;

	// printf("%16s \t%d \t %d\n",tmp, t_class, t_value);

	ptr = (token_list*)calloc(1, sizeof(token_list));
	strcpy(ptr->tok_string, tmp);
	ptr->tok_class = t_class;
	ptr->tok_value = t_value;
	ptr->next = NULL;

  if (cur == NULL)
		*tok_list = ptr;
	else
	{
		while (cur->next != NULL)
			cur = cur->next;

		cur->next = ptr;
	}
	return;
}

int do_semantic(token_list *tok_list)
{
	int rc = 0, cur_cmd = INVALID_STATEMENT;
	bool unique = false;
  token_list *cur = tok_list;

	if ((cur->tok_value == K_CREATE) &&
			((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("CREATE TABLE statement\n");
		cur_cmd = CREATE_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_DROP) &&
					((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("DROP TABLE statement\n");
		cur_cmd = DROP_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) &&
					((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("LIST TABLE statement\n");
		cur_cmd = LIST_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) &&
					((cur->next != NULL) && (cur->next->tok_value == K_SCHEMA)))
	{
		printf("LIST SCHEMA statement\n");
		cur_cmd = LIST_SCHEMA;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_INSERT) &&
					((cur->next != NULL) && (cur->next->tok_value == K_INTO)))
	{
		printf("INSERT INTO statement\n");
		cur_cmd = INSERT;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_SELECT))
	{
		printf("SELECT statement\n");
		cur_cmd = SELECT;
		cur = cur->next;
	}
	else if ((cur->tok_value == K_UPDATE))
	{
		printf("UPDATE statement\n");
		cur_cmd = UPDATE;
		cur = cur->next;
	}
	else if ((cur->tok_value == K_DELETE)  &&
					((cur->next != NULL) && (cur->next->tok_value == K_FROM)))
	{
		printf("DELETE FROM statement\n");
		cur_cmd = DELETE;
		cur = cur->next;
	}
	else
  {
		printf("Invalid statement\n");
		rc = cur_cmd;
	}

	if (cur_cmd != INVALID_STATEMENT)
	{
		switch(cur_cmd)
		{
			case CREATE_TABLE:
						rc = sem_create_table(cur);
						break;
			case DROP_TABLE:
						rc = sem_drop_table(cur);
						break;
			case LIST_TABLE:
						rc = sem_list_tables();
						break;
			case LIST_SCHEMA:
						rc = sem_list_schema(cur);
						break;
			case INSERT:
						rc = sem_insert_into(cur);
						break;
			case SELECT:
						rc = sem_select(cur);
						break;
			case DELETE:
						rc = sem_delete_from(cur);
						break;
			case UPDATE:
						rc = sem_update(cur);
						break;
			default:
					; /* no action */
		}
	}
	
	return rc;
}

int sem_create_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry tab_entry;
	tpd_entry *new_entry = NULL;
	bool column_done = false;
	int cur_id = 0;
	cd_entry	col_entry[MAX_NUM_COL];
	int rowSize=0;

	memset(&tab_entry, '\0', sizeof(tpd_entry));
	cur = t_list;
	if ((cur->tok_class != keyword) &&
		  (cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if ((new_entry = get_tpd_from_list(cur->tok_string)) != NULL)
		{
			rc = DUPLICATE_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			strcpy(tab_entry.table_name, cur->tok_string);
			cur = cur->next;
			if (cur->tok_value != S_LEFT_PAREN)
			{
				//Error
				rc = INVALID_TABLE_DEFINITION;
				cur->tok_value = INVALID;
			}
			else
			{
				memset(&col_entry, '\0', (MAX_NUM_COL * sizeof(cd_entry)));

				/* Now build a set of column entries */
				cur = cur->next;
				do
				{
					if ((cur->tok_class != keyword) &&
							(cur->tok_class != identifier) &&
							(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_COLUMN_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						int i;
						for(i = 0; i < cur_id; i++)
						{
              /* make column name case sensitive */
							if (strcmp(col_entry[i].col_name, cur->tok_string)==0)
							{
								rc = DUPLICATE_COLUMN_NAME;
								cur->tok_value = INVALID;
								break;
							}
						}

						if (!rc)
						{
							strcpy(col_entry[cur_id].col_name, cur->tok_string);
							col_entry[cur_id].col_id = cur_id;
							col_entry[cur_id].not_null = false;    /* set default */

							cur = cur->next;
							if (cur->tok_class != type_name)
							{
								// Error
								rc = INVALID_TYPE_NAME;
								cur->tok_value = INVALID;
							}
							else
							{
                /* Set the column type here, int or char */
								col_entry[cur_id].col_type = cur->tok_value;
								cur = cur->next;
		
								if (col_entry[cur_id].col_type == T_INT)
								{
									rowSize+=4;
									if ((cur->tok_value != S_COMMA) &&
										  (cur->tok_value != K_NOT) &&
										  (cur->tok_value != S_RIGHT_PAREN))
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
								  else
									{
										col_entry[cur_id].col_len = sizeof(int);
										
										if ((cur->tok_value == K_NOT) &&
											  (cur->next->tok_value != K_NULL))
										{
											rc = INVALID_COLUMN_DEFINITION;
											cur->tok_value = INVALID;
										}	
										else if ((cur->tok_value == K_NOT) &&
											    (cur->next->tok_value == K_NULL))
										{					
											col_entry[cur_id].not_null = true;
											cur = cur->next->next;
										}
	
										if (!rc)
										{
											/* I must have either a comma or right paren */
											if ((cur->tok_value != S_RIGHT_PAREN) &&
												  (cur->tok_value != S_COMMA))
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
		                  {
												if (cur->tok_value == S_RIGHT_PAREN)
												{
 													column_done = true;
												}
												cur = cur->next;
											}
										}
									}
								}   // end of T_INT processing
								else
								{
									// It must be char() or varchar() 
									if (cur->tok_value != S_LEFT_PAREN)
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
									else
									{
										/* Enter char(n) processing */
										cur = cur->next;
										if(IS_DEBUG) printf("col len parsing\n");
										if (cur->tok_value != INT_LITERAL)
										{
											rc = INVALID_COLUMN_LENGTH;
											cur->tok_value = INVALID;
										}
										else
										{
											/* Got a valid integer - convert */
											col_entry[cur_id].col_len = atoi(cur->tok_string);
											cur = cur->next;
											if(IS_DEBUG) printf("col len - %d\n", col_entry[cur_id].col_len);
											rowSize+=col_entry[cur_id].col_len;
											if (cur->tok_value != S_RIGHT_PAREN)
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
											{
												cur = cur->next;
						
												if ((cur->tok_value != S_COMMA) &&
														(cur->tok_value != K_NOT) &&
														(cur->tok_value != S_RIGHT_PAREN))
												{
													rc = INVALID_COLUMN_DEFINITION;
													cur->tok_value = INVALID;
												}
												else
												{
													if ((cur->tok_value == K_NOT) &&
														  (cur->next->tok_value != K_NULL))
													{
														rc = INVALID_COLUMN_DEFINITION;
														cur->tok_value = INVALID;
													}
													else if ((cur->tok_value == K_NOT) &&
																	 (cur->next->tok_value == K_NULL))
													{					
														col_entry[cur_id].not_null = true;
														cur = cur->next->next;
													}
		
													if (!rc)
													{
														/* I must have either a comma or right paren */
														if ((cur->tok_value != S_RIGHT_PAREN) &&															  (cur->tok_value != S_COMMA))
														{
															rc = INVALID_COLUMN_DEFINITION;
															cur->tok_value = INVALID;
														}
														else
													  {
															if (cur->tok_value == S_RIGHT_PAREN)
															{
																column_done = true;
															}
															cur = cur->next;
														}
													}
												}
											}
										}	/* end char(n) processing */
									}
								} /* end char processing */
							}
						}  // duplicate column name
					} // invalid column name

					/* If rc=0, then get ready for the next column */
					if (!rc)
					{
						cur_id++;
					}

				} while ((rc == 0) && (!column_done));
	
				if ((column_done) && (cur->tok_value != EOC))
				{
					rc = INVALID_TABLE_DEFINITION;
					cur->tok_value = INVALID;
				}

				if (!rc)
				{
					/* Now finished building tpd and add it to the tpd list */
					tab_entry.num_columns = cur_id;
					tab_entry.tpd_size = sizeof(tpd_entry) + sizeof(cd_entry) *	tab_entry.num_columns;
					tab_entry.cd_offset = sizeof(tpd_entry);
					new_entry = (tpd_entry*)calloc(1, tab_entry.tpd_size);

					if (new_entry == NULL)
					{
						rc = MEMORY_ERROR;
					}
					else
					{
						memcpy((void*)new_entry,
							     (void*)&tab_entry,
									 sizeof(tpd_entry));
		
						memcpy((void*)((char*)new_entry + sizeof(tpd_entry)),
									 (void*)col_entry,
									 sizeof(cd_entry) * tab_entry.num_columns);
	
						rc = add_tpd_to_list(new_entry);

						free(new_entry);
						
						//create tableName.tab file
						table_file_header* tfh = (table_file_header*)calloc(1, sizeof(table_file_header));
						if(IS_DEBUG) printf("record size = %d+%d\n",rowSize, tab_entry.num_columns);
						tfh->record_size = rowSize + tab_entry.num_columns;
						tfh->num_records=0;
						tfh->record_offset=sizeof(table_file_header);//no. of parameters in table_file_header
						//std::ofstream MyFile(strcat(tab_entry.table_name, ".tab"));
						//MyFile.close(); 
						FILE *fhandle = NULL;
						if((fhandle = fopen(strcat(tab_entry.table_name, ".tab"), "wbc")) == NULL)
						{
							rc = FILE_OPEN_ERROR;
							printf("file open issue");
						}
						else
						{
							if(IS_DEBUG) printf("struct size - %d\n",sizeof(tfh));
							fwrite(tfh, sizeof(table_file_header), 1, fhandle);
							fflush(fhandle);
							fclose(fhandle);
						}
					}
				}
			}
		}
	}
  return rc;
}

int sem_drop_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;

	cur = t_list;
	if ((cur->tok_class != keyword) &&
		  (cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if (cur->next->tok_value != EOC)
		{
			rc = INVALID_STATEMENT;
			cur->next->tok_value = INVALID;
		}
		else
		{
			if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
			{
				rc = TABLE_NOT_EXIST;
				cur->tok_value = INVALID;
			}
			else
			{
				/* Found a valid tpd, drop it from tpd list */
				rc = drop_tpd_from_list(cur->tok_string);
			}
		}
	}

  return rc;
}

int sem_list_tables()
{
	int rc = 0;
	int num_tables = g_tpd_list->num_tables;
	tpd_entry *cur = &(g_tpd_list->tpd_start);

	if (num_tables == 0)
	{
		printf("\nThere are currently no tables defined\n");
	}
	else
	{
		printf("\nTable List\n");
		printf("*****************\n");
		while (num_tables-- > 0)
		{
			printf("%s\n", cur->table_name);
			if (num_tables > 0)
			{
				cur = (tpd_entry*)((char*)cur + cur->tpd_size);
			}
		}
		printf("****** End ******\n");
	}

  return rc;
}

int sem_list_schema(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	cd_entry  *col_entry = NULL;
	char tab_name[MAX_IDENT_LEN+1];
	char filename[MAX_IDENT_LEN+1];
	bool report = false;
	FILE *fhandle = NULL;
	int i = 0;

	cur = t_list;

	if (cur->tok_value != K_FOR)
  {
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
	}
	else
	{
		cur = cur->next;

		if ((cur->tok_class != keyword) &&
			  (cur->tok_class != identifier) &&
				(cur->tok_class != type_name))
		{
			// Error
			rc = INVALID_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			memset(filename, '\0', MAX_IDENT_LEN+1);
			strcpy(tab_name, cur->tok_string);
			cur = cur->next;

			if (cur->tok_value != EOC)
			{
				if (cur->tok_value == K_TO)
				{
					cur = cur->next;
					
					if ((cur->tok_class != keyword) &&
						  (cur->tok_class != identifier) &&
							(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_REPORT_FILE_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						if (cur->next->tok_value != EOC)
						{
							rc = INVALID_STATEMENT;
							cur->next->tok_value = INVALID;
						}
						else
						{
							/* We have a valid file name */
							strcpy(filename, cur->tok_string);
							report = true;
						}
					}
				}
				else
				{ 
					/* Missing the TO keyword */
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
				}
			}

			if (!rc)
			{
				if ((tab_entry = get_tpd_from_list(tab_name)) == NULL)
				{
					rc = TABLE_NOT_EXIST;
					cur->tok_value = INVALID;
				}
				else
				{
					if (report)
					{
						if((fhandle = fopen(filename, "a+tc")) == NULL)
						{
							rc = FILE_OPEN_ERROR;
						}
					}

					if (!rc)
					{
						/* Find correct tpd, need to parse column and index information */

						/* First, write the tpd_entry information */
						printf("Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
						printf("Table Name               (table_name)  = %s\n", tab_entry->table_name);
						printf("Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
						printf("Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
            printf("Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 

						if (report)
						{
							fprintf(fhandle, "Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
							fprintf(fhandle, "Table Name               (table_name)  = %s\n", tab_entry->table_name);
							fprintf(fhandle, "Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
							fprintf(fhandle, "Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
              fprintf(fhandle, "Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 
						}

						/* Next, write the cd_entry information */
						for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
								i < tab_entry->num_columns; i++, col_entry++)
						{
							printf("Column Name   (col_name) = %s\n", col_entry->col_name);
							printf("Column Id     (col_id)   = %d\n", col_entry->col_id);
							printf("Column Type   (col_type) = %d\n", col_entry->col_type);
							printf("Column Length (col_len)  = %d\n", col_entry->col_len);
							printf("Not Null flag (not_null) = %d\n\n", col_entry->not_null);

							if (report)
							{
								fprintf(fhandle, "Column Name   (col_name) = %s\n", col_entry->col_name);
								fprintf(fhandle, "Column Id     (col_id)   = %d\n", col_entry->col_id);
								fprintf(fhandle, "Column Type   (col_type) = %d\n", col_entry->col_type);
								fprintf(fhandle, "Column Length (col_len)  = %d\n", col_entry->col_len);
								fprintf(fhandle, "Not Null Flag (not_null) = %d\n\n", col_entry->not_null);
							}
						}
	
						if (report)
						{
							fflush(fhandle);
							fclose(fhandle);
						}
					} // File open error							
				} // Table not exist
			} // no semantic errors
		} // Invalid table name
	} // Invalid statement

  return rc;
}

int sem_insert_into(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	bool column_done = false;
	int cur_id = 0;
	cd_entry  *col_entry = NULL;
	
	cur = t_list;
	if ((cur->tok_class != keyword) &&
		  (cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
		{
			rc = TABLE_NOT_EXIST;
			cur->tok_value = INVALID;
		}
		else
		{
			cur = cur->next;
			if(cur->tok_value!=K_VALUES)
			{
				rc = INVALID_SYNTAX;
				cur->tok_value = INVALID;
			}
			else
			{
				cur = cur->next;
				if (cur->tok_value != S_LEFT_PAREN)
				{
					//Error
					rc = INVALID_INSERT_DEFINITION;
					cur->tok_value = INVALID;
				}
				else
				{
					FILE *fhandle = NULL;
					table_file_header* tfh = (table_file_header*)calloc(1, sizeof(table_file_header));
					char* tname=(char*)calloc(1, MAX_TOK_LEN);
					memcpy(tname, tab_entry->table_name, MAX_TOK_LEN);
					strcat(tname, ".tab");
					if(IS_DEBUG) printf("table name - %s\n",tname);
					if((fhandle = fopen(tname, "rbc")) == NULL)
					{
						rc = FILE_OPEN_ERROR;
						printf("file open issue");
					}
					else
					{
						if(IS_DEBUG) printf("sizeof(table_file_header) - %d\n", sizeof(table_file_header));
						struct stat file_stat;
						fstat(fileno(fhandle), &file_stat);
						fread(tfh, sizeof(table_file_header), 1, fhandle);
						if(IS_DEBUG) printf("file size = %d\nno. of records = %d\n", file_stat.st_size,tfh->num_records);
						//fflush(fhandle);

						char* filedata = (char*)calloc(1, file_stat.st_size);

						fread(filedata, file_stat.st_size, 1, fhandle);
						//fflush(fhandle);
						if(IS_DEBUG) printCharArrInInt(filedata, file_stat.st_size);
						if(IS_DEBUG) printf("\n in char format - ");
						if(IS_DEBUG) printCharArr(filedata, file_stat.st_size);
						//fflush(fhandle);
						fclose(fhandle);
						if(IS_DEBUG) printf("num records - %d\nrecord length - %d\n",tfh->num_records, tfh->record_size);
						char pastRecords[tfh->num_records][tfh->record_size];
						char rowdata[tfh->record_size];
						int offset=0;
						
						
						for(int i=0;i<tfh->num_records;i++){
							if(IS_DEBUG) printf("record size - %d\n", tfh->record_size);
							//fread(pastRecords[i], tfh->record_size, 1, fhandle);
							memcpy(pastRecords[i], filedata+i*tfh->record_size, tfh->record_size);
							if(IS_DEBUG) printCharArrInInt(pastRecords[i], tfh->record_size);
							if(IS_DEBUG) printf("\n");
						}
						
						
						
						cur = cur->next;
						col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
						for(int i = 0; i < tab_entry->num_columns; i++, col_entry++){
							if(IS_DEBUG) printf("Column Name   (col_name) = %s\n", col_entry->col_name);
							if(IS_DEBUG) printf("Column Id     (col_id)   = %d\n", col_entry->col_id);
							if(IS_DEBUG) printf("Column Type   (col_type) = %d\n", col_entry->col_type);
							if(IS_DEBUG) printf("Column Length (col_len)  = %d\n", col_entry->col_len);
							if(IS_DEBUG) printf("Not Null flag (not_null) = %d\n\n", col_entry->not_null);
							//strcpy(tab_entry->table_name, cur->tok_string);
							if(IS_DEBUG) printf("reading - '%s'\noffset - %d\n", cur->tok_string, offset);
							
							
							if(cur->tok_value==K_NULL){
								*(rowdata + offset)=0;
								if(col_entry->not_null){
									rc = NULL_NOT_ALLOWED;
									cur->tok_value = INVALID;
									return rc;
								}
							}
							else
								*(rowdata + offset)=col_entry->col_len;
							offset++;
							
							
							if(col_entry->col_type==T_INT){
								if (cur->tok_value != INT_LITERAL && cur->tok_value!=K_NULL)
								{
									rc = INVALID_INSERT_STATEMENT;
									cur->tok_value = INVALID;
									return rc;
								}
								else
								{
									if(IS_DEBUG) printf("check int1 - %d\n",cur->tok_string[0]);
									if(cur->tok_value!=K_NULL){
										int n = atoi(cur->tok_string);
										*(rowdata + offset + 3) = (n >> 24) & 0xFF;
										*(rowdata + offset + 2) = (n >> 16) & 0xFF;
										*(rowdata + offset + 1) = (n >> 8) & 0xFF;
										*(rowdata + offset + 0) = n & 0xFF;
										//printf("bin val- %#02x\n",*(rowdata + offset + 3));
									}
									//printCharArr(*(rowdata + offset + 0), 4);
									//copyBytes((rowdata + offset), cur->tok_string, col_entry->col_len);
									if(IS_DEBUG) printf("check int2\n");
									offset+=col_entry->col_len;
									//rowdata[i].data=cur->tok_string;
								}
							}
							else if(col_entry->col_type==T_CHAR){
								if(strlen(cur->tok_string)>col_entry->col_len){
									rc = MAX_LENGTH_EXCEEDED;
									cur->tok_value = INVALID;
									return rc;
								}
								
								if (cur->tok_value != STRING_LITERAL && cur->tok_value!=K_NULL)
								{
									//Error
									rc = INVALID_TABLE_DEFINITION;
									cur->tok_value = INVALID;
									return rc;
								}
								else
								{
									if(cur->tok_value!=K_NULL)
										copyBytes((rowdata + offset), cur->tok_string, col_entry->col_len);
									offset+=col_entry->col_len;
								}
							}
							
							cur = cur->next;
							if(IS_DEBUG) printf("%d\n", cur->tok_value);
							if (cur->tok_value != S_COMMA )
							{
								if(cur->tok_value == S_RIGHT_PAREN){
									if(IS_DEBUG) printf("closing%d, %d\n", i, tab_entry->num_columns-1);
									if(i == tab_entry->num_columns-1)
										break;
									else{
										rc=INCOMPLETE_INSERT_STATEMENT;
										cur->tok_value = INVALID;
										return rc;
									}
								}
								rc = INVALID_INSERT_STATEMENT;
								cur->tok_value = INVALID;
								return rc;
							}
							cur = cur->next;
						}
						if(IS_DEBUG) printf("new row parsed\n");
						if(IS_DEBUG) printf("new row - ");
						if(IS_DEBUG) printCharArrInInt(rowdata, tfh->record_size);
						if(cur->tok_value != S_RIGHT_PAREN){
							rc = INVALID_INSERT_STATEMENT;
							cur->tok_value = INVALID;
							return rc;
						}
						else if(rc!=0){
							
						}
						else{
							fhandle = NULL;
							if((fhandle = fopen(tname, "w")) == NULL)
							{
								rc = FILE_OPEN_ERROR;
								if(IS_DEBUG) printf("file open issue");
							}
							else
							{
								if(IS_DEBUG) printf("\nwriting to file. past data -\n");
								
								//if(IS_DEBUG) printf("current file size-%d\n",file_stat.st_size);
								tfh->num_records++;
								//if(IS_DEBUG) printf("num records - %d\nrecord length - %d\n",tfh->num_records, tfh->record_size);
								fwrite(tfh, sizeof(table_file_header), 1, fhandle);
								fflush(fhandle);
								for(int i=0;i<tfh->num_records-1;i++){
									fwrite(pastRecords[i], tfh->record_size, 1, fhandle);
									fflush(fhandle);
									if(IS_DEBUG) printCharArrInInt(pastRecords[i], tfh->record_size);
									if(IS_DEBUG) printf("\n");
								}
								if(IS_DEBUG) printf("\n------------------------------\n");
								fwrite(rowdata, sizeof(rowdata), 1, fhandle);
								fflush(fhandle);
								fclose(fhandle);
							}
						}
					}
				}
			}
		}
	}
  return rc;
}

int sem_select(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	bool column_done = false;
	int cur_id = 0;
	cd_entry  *col_entry = NULL;
	select_attribute** columnsInSelect = NULL;
	//select_attribute** columnListToPrint = NULL;
	int columnCountInSelect=0;
	cur = t_list;
	columnsInSelect = (select_attribute**)calloc(100,sizeof(select_attribute*));
	columnCountInSelect = getColumnList(cur, columnsInSelect); 
	//printf("[sem_select] columnCountInSelect - %d\n", columnCountInSelect);
	if(columnCountInSelect<0){
		rc = columnCountInSelect;
	}
	else
	{
		while(cur->tok_value!=K_FROM)
			cur = cur->next;
		if(cur->tok_value!=K_FROM)
		{
			rc = INVALID_SYNTAX;
			cur->tok_value = INVALID;
		}
		else
		{
			//printf("[sem_select] reading tpd file\n");
			cur = cur->next;
			if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
			{
				rc = TABLE_NOT_EXIST;
				cur->tok_value = INVALID;
			}
			else
			{

				//columnListToPrint = (select_attribute**)calloc(100, sizeof(select_attribute*));
				//int columnCount = filterColumns(columnsInSelect,columnCountInSelect,tab_entry, columnListToPrint);
				int columnObjUpdated = filterColumns(columnsInSelect,columnCountInSelect,tab_entry);
				//printf("[sem_select] columnCount - %d\n", columnObjUpdated);
				
				if(columnObjUpdated<0){
					rc = columnObjUpdated;
					cur->tok_value = INVALID;
				}
				else{
					table_file_header* tfh = (table_file_header*)calloc(1, sizeof(table_file_header));
					char** records=getTableData(tab_entry, tfh);
					if(records==NULL){
						cur->tok_value = INVALID;
						return FILE_OPEN_ERROR;
					}
					char rowdata[tfh->record_size];
					bool* rowsToPrint=NULL;
					int offset=0;
					int* groupNos=NULL;
					row_obj** rows = NULL;
					int rowCount=0;
					char** updatedRows = NULL;

					cur = cur->next;
					if(cur->tok_value == K_WHERE)
					{
						cur = cur->next;
						condition* conditionList = (condition*)calloc(1, 100);
						int conditionCount = parseWhereClause(cur, tab_entry, conditionList);
						if(conditionCount<0){
							rc = conditionCount;
							return rc;
						}

						//check for aggregate functions

						rowsToPrint=filterRows(records, tab_entry, conditionList, conditionCount, tfh->num_records, tfh->record_size);
						//printf("%s\n", cur->tok_string);
						while(cur->tok_value !=  K_ORDER && cur->tok_value !=  EOC){
							//printf("%s\n", cur->tok_string);
							cur=cur->next;
						}
					}
					//printf("checkpt\n");

					if(cur->tok_value == K_NATURAL){
						cur=cur->next;
						if(cur->tok_value == K_JOIN){
							cur=cur->next;
							tpd_entry* tab_entry_new_table = get_tpd_from_list(cur->tok_string);
							table_file_header* tfh_new=(table_file_header*)calloc(1, sizeof(table_file_header));
							if(tab_entry_new_table!=NULL){
								char** newRowData=getTableData(tab_entry_new_table,tfh_new);
								printf("table file read finished\n");
								//int** commonIndices=(int**)calloc(10,sizeof(int*));
								if(newRowData!=NULL){
									printf("table file read success\n");
									updatedRows=(char**)calloc(tfh->num_records*tfh_new->num_records,sizeof(char*));
									int columnObjUpdated = filterColumns(columnsInSelect,columnCountInSelect,tab_entry_new_table);
									rowCount=getJoinedData(tab_entry, tab_entry_new_table, records,newRowData, tfh, tfh_new, columnsInSelect, columnCountInSelect, updatedRows);
									printf("new rows-%d\n", rowCount);
									if(rowCount>0){

									}else{

									}
								}else{
									rc=FILE_OPEN_ERROR;
									cur->tok_value = INVALID;
									return rc;									
								}
							}else{
								rc=INVALID_TABLE_NAME;
								cur->tok_value = INVALID;
								return rc;
							}
							cur=cur->next;
						}else{
							rc=INVALID_SYNTAX;
							cur->tok_value = INVALID;
							return rc;
						}
					}

					if(cur->tok_value == K_GROUP){
						cur=cur->next;
						if(cur->tok_value == K_BY){
							cur=cur->next;
							col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
							int columnOffset=0;
							int colType=0;
							int columnIndex = columnExists(cur->tok_string,tab_entry);
							if(columnIndex<0){
								rc=COLUMN_NOT_EXIST;
								cur->tok_value = INVALID;
								return rc;
							}
							columnOffset=getRowOffset(tab_entry,columnIndex);

							rows = (row_obj**)calloc(tfh->num_records,sizeof(row_obj*));
							for(int i=0;i<tfh->num_records;i++){
								rows[i] =  (row_obj*)calloc(1,sizeof(row_obj));
								rows[i]->rowData=records[i];
								rows[i]->offset=columnOffset;
								rows[i]->dataType=colType;
								//printf("creating row obj - %d\n",i);
								if(rowsToPrint!=NULL)
									rows[i]->shouldPrint=rowsToPrint[i];
							}
							std::sort(rows, rows+tfh->num_records, [](row_obj* a, row_obj* b)-> bool {
								return a->lessThan(*b);
							});
							int groupNo=0;
							groupNos=(int*)calloc(tfh->num_records,sizeof(int));
							for(int i=0;i<tfh->num_records;i++){
								records[i] = rows[i]->rowData;
								if(rowsToPrint!=NULL) rowsToPrint[i] = rows[i]->shouldPrint;
								//printf("group no - %d\n",groupNo);
								//printCharArrInInt(records[i], tfh->record_size);
								if(i>0){
									if(!rows[i]->equals(*(rows[i-1])))
										groupNo++;
								}
								groupNos[i]=groupNo;
							}
							rowCount=groupNo+1;
							updatedRows = (char**)calloc(rowCount,sizeof(char*));
							//printf("creating pointer-%d\n", rowCount);
							//int newRowLen = getRowLen(columnsInSelect, columnCount);
							int newRowLen = columnCountInSelect + columnCountInSelect*32;//getRowLen(columnsInSelect, columnCount);
							//SELECT COUNT(CustomerID) FROM Customers GROUP BY Country;
							//SELECT COUNT(CustomerID), Country FROM Customers GROUP BY Country;
							int offset=0;
							groupNo=0;
							for(int i=0;i<rowCount;i++){
								updatedRows[i]=(char*)calloc(newRowLen,sizeof(char));
								//printf("creating memory-%d, i %d\n", newRowLen, i);
							}
							for(int i=0;i<columnCountInSelect;i++){
								//printf("column no. -%d, offset-%d\n", i, columnsInSelect[i]->columnOffset);
								columnsInSelect[i]->columnLength=32;
								columnsInSelect[i]->columnOffset=offset;
								offset++;
								//printf("getting offset - %d\n",offset);
								if(columnsInSelect[i]->functionType!=NONE){
									//printf("[sem_select]func type - %d %d\n", columnsInSelect[i]->functionType, AVG);
									groupNo=0;
									for(int rowNo=0;rowNo<tfh->num_records;rowNo++){
										int cellOffset = getRowOffset(tab_entry,columnsInSelect[i]->columnIndex);
										int sum=bin2int(records[rowNo]+cellOffset+1);
										int avgCount=1;
										int j;
										bool isNull=false;
										//printf("aggregate rowNo-%d, sum - %d\n", rowNo, sum);
										for(j=rowNo+1;j<tfh->num_records && groupNos[j]==groupNo;j++){
											//printf("sum-%d\n", bin2int(records[j]+cellOffset+1));
											int cellLen = *(records[j]+cellOffset);
											if(cellLen==0){
												isNull=true;
											}
											if(cellLen==0||(rowsToPrint!=NULL && !rowsToPrint[j])) continue;
											sum+=bin2int(records[j]+cellOffset+1);
											avgCount++;
											//printf("end avgCount-%d\n", avgCount);
											
										}
										rowNo=j-1;
										if(isNull)
											*(updatedRows[groupNo]+offset-1)=0;
										else
											*(updatedRows[groupNo]+offset-1)=32;
										float average=sum*1.0f/avgCount;
										//printf("exit\n");
										//printf("before index %d offset %d\n",rowNo, offset);
										if(columnsInSelect[i]->functionType==AVG){
											sprintf(updatedRows[groupNo] + offset, "%2f", average);
											//printf("AVG(%s)\n\t---------\n\t", columnsInSelect[i]->columnName);								
										}
										else if(columnsInSelect[i]->functionType==SUM){
											sprintf(updatedRows[groupNo] + offset, "%d", sum);
											//printf("SUM(%s)\n\t---------\n\t", columnsInSelect[i]->columnName);
										}
										else if(columnsInSelect[i]->functionType==COUNT){
											sprintf(updatedRows[groupNo] + offset, "%d", avgCount);
											/*printf("COUNT(%s)\n\t---------\n\t", columnsInSelect[i]->columnName);
											if(columnsInSelect[i]->columnName[0]=='*')
												printf("%d\n", tfh->num_records);
											else
												printf("%d\n", avgCount);*/
										}
										//printf("data written - %s\n",updatedRows[groupNo] + offset);
										groupNo++;
									}
								}else{
									groupNo=0;
									int offset1 = getRowOffset(tab_entry,columnsInSelect[i]->columnIndex)+1;
									for(int rowNo=0;rowNo<rowCount;rowNo++){
										int sum=0;
										int avgCount=0;
										int j=0;

										*(updatedRows[groupNo]+offset-1)=columnsInSelect[i]->columnLength;

										//printf("before index %d offset %d offset1 %d groupNo %d\n",rowNo, offset, offset1, groupNo);
										for(j=rowNo+1;groupNos[j]==groupNo && j<rowCount;j++);
										rowNo=j-1;
										//printf("index %d offset %d offset1 %d groupNo %d\n",rowNo, offset, offset1, groupNo);

										printf("getting %d\n",bin2int(records[rowNo]+offset1));
										printf("type-%d\n", (col_entry+columnsInSelect[i]->columnIndex)->col_type);
										if((col_entry+columnsInSelect[i]->columnIndex)->col_type==T_CHAR)
											sprintf(updatedRows[groupNo] + offset, "%s", records[rowNo]+offset1);
										else{
											//printCharArrInInt(records[rowNo]+offset1,4);
											copyBytes(updatedRows[groupNo] + offset, records[rowNo]+offset1,4);
											//printCharArrInInt(updatedRows[groupNo] + offset,4);
											//memcpy(updatedRows[groupNo] + offset,records[rowNo]+offset1,4);
										}
										groupNo++;
									}
									printf("exit, group-%d\n",groupNo);
								}
								offset=offset+32;
							}
							cur=cur->next;
						}else{
							rc=INVALID_SYNTAX;
							cur->tok_value = INVALID;
							return rc;
						}
					}


					if(cur->tok_value == K_ORDER){
						cur=cur->next;
						if(cur->tok_value == K_BY){
							cur=cur->next;
							col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
							int columnOffset=0;
							int colType=0;
							int columnIndex = columnExists(cur->tok_string,tab_entry);
							if(columnIndex<0){
								rc=COLUMN_NOT_EXIST;
								cur->tok_value = INVALID;
								return rc;
							}
							columnOffset=getRowOffset(tab_entry,columnIndex);
							if(rows==NULL){
								//printf("creating rows\n");
								rows = (row_obj**)calloc(tfh->num_records,sizeof(row_obj*));
								for(int i=0;i<tfh->num_records;i++){
									//printf("creating rows - %d\n", i);
									rows[i] =  (row_obj*)calloc(1,sizeof(row_obj));
									rows[i]->rowData=records[i];
									rows[i]->offset=columnOffset;
									rows[i]->dataType=colType;
									if(rowsToPrint!=NULL)
										rows[i]->shouldPrint=rowsToPrint[i];
								}
							}
							std::sort(rows, rows+tfh->num_records, [](row_obj* a, row_obj* b)-> bool {
								return a->lessThan(*b);
							});
							for(int i=0;i<tfh->num_records;i++){
								//printf("saving rows - %d\n", i);
								records[i] = rows[i]->rowData;
								if(rowsToPrint!=NULL) rowsToPrint[i] = rows[i]->shouldPrint;
								//printCharArrInInt(records[i], tfh->record_size);
							}
						}else{
							rc=INVALID_SYNTAX;
							cur->tok_value = INVALID;
							return rc;
						}
					}else if(cur->tok_value != EOC){
						rc=INVALID_SYNTAX;
						cur->tok_value = INVALID;
						return rc;
					}
					
					printf("\n\t");
					printColumnList(columnsInSelect, tab_entry, columnCountInSelect);
					printf("\n\t");
					printDashes(columnsInSelect, tab_entry, columnCountInSelect);
					printf("\n\t");
					int numRecords=tfh->num_records;
					char** recordsToPrint=records;
					if(updatedRows!=NULL){//grouping is done
						numRecords=rowCount;
						recordsToPrint=updatedRows;
					}
					printRowData(columnsInSelect, columnCountInSelect, numRecords, tab_entry, rowsToPrint, recordsToPrint);
					
				}
			}
		}
	}
  return rc;
}

int getJoinedData(tpd_entry* tab_entry1, tpd_entry* tab_entry2, char** records1, char** records2, table_file_header* tfh1, table_file_header* tfh2, select_attribute** columnsInSelect, int columnCount, char** outputRows){
	cd_entry* col_entry1 = (cd_entry*)((char*)tab_entry1 + tab_entry1->cd_offset);
	cd_entry* col_entry2 = (cd_entry*)((char*)tab_entry2 + tab_entry2->cd_offset);
	int** commonIndices=(int**)calloc(10,sizeof(int*));
	int commonColumnCount=0;
	for(int i=0;i<tab_entry1->num_columns;i++){
		for(int j=0;j<tab_entry2->num_columns;j++){
			if(strcasecmp((col_entry1+i)->col_name, (col_entry2+j)->col_name)==0 && ((col_entry1+i)->col_type==(col_entry2+j)->col_type)){
				commonIndices[commonColumnCount]=(int*)calloc(2,sizeof(int));
				commonIndices[commonColumnCount][0]=i;
				commonIndices[commonColumnCount][1]=j;
				//printf("common column %s, %s\n",(col_entry1+i)->col_name, (col_entry2+j)->col_name);
				commonColumnCount++;
			}
		}
	}
	printf("common column count %d\n",commonColumnCount);
	int outputRowCount=0;
	for(int row1=0;row1<tfh1->num_records;row1++){
		for(int row2=0;row2<tfh2->num_records;row2++){
			bool allMatch=true;
			for(int i=0;i<commonColumnCount;i++){
				int row1CellOffset=getRowOffset(tab_entry1,commonIndices[i][0])+1;
				int row2CellOffset=getRowOffset(tab_entry2,commonIndices[i][1])+1;
				//printf("data compare %s, %s\n",records1[row1]+row1CellOffset, records2[row2]+row2CellOffset);
				if(strcmp(records1[row1]+row1CellOffset, records2[row2]+row2CellOffset)!=0){
					allMatch=false;
					break;
				}
			}
			if(allMatch){
				int offset=0;
				outputRows[outputRowCount]=(char*)calloc(getRowLen(columnsInSelect,columnCount)+1*columnCount,sizeof(char));
				printf("size per row - %d + %d\n", getRowLen(columnsInSelect,columnCount),1*columnCount);
				//printCharArr(records1[row1], 40);
				//printCharArr(records2[row2], 40);
				for(int c=0;c<columnCount;c++){
					int rowOffset=columnsInSelect[c]->columnOffset+1;//getRowOffset(tab_entry1,columnsInSelect[c]->columnOffset)+1;
					//printf("table name %s \n",columnsInSelect[c]->tableName);
					if(strcasecmp(columnsInSelect[c]->tableName,tab_entry1->table_name)==0){
						*(outputRows[outputRowCount]+offset)=columnsInSelect[c]->columnLength;
						offset++;
						//printf("[getJoinedData] t1 adding % 20s % 10d offset-%d length-%d\n",records1[row1]+rowOffset, bin2int(records1[row1]+rowOffset), offset, *(outputRows[outputRowCount]+offset-1));
						copyBytes(outputRows[outputRowCount]+offset, records1[row1]+rowOffset, columnsInSelect[c]->columnLength);
						//printf("t1 adding %d\n", bin2int(outputRows[outputRowCount]+offset));
						offset+=columnsInSelect[c]->columnLength;
					}
					else if(strcasecmp(columnsInSelect[c]->tableName,tab_entry2->table_name)==0){
						*(outputRows[outputRowCount]+offset)=columnsInSelect[c]->columnLength;
						offset++;
						//printf("[getJoinedData] t2 adding % 20s % 10d offset %d length-%d\n",records2[row2]+rowOffset,  bin2int(records2[row2]+rowOffset), offset, *(outputRows[outputRowCount]+offset-1));
						copyBytes(outputRows[outputRowCount]+offset, records2[row2]+rowOffset, columnsInSelect[c]->columnLength);
						//printf("t2 adding %d\n",  bin2int(outputRows[outputRowCount]+offset));
						offset+=columnsInSelect[c]->columnLength;
					}else{
						//printf("t3 adding %s \n",columnsInSelect[c]->tableName);
						*(outputRows[outputRowCount]+offset)=0;
						offset+=columnsInSelect[c]->columnLength+1;
					}
					//columnsInSelect[c]->columnLength=32;
				}
				printCharArrInInt(outputRows[outputRowCount],getRowLen(columnsInSelect,columnCount)+1*columnCount);
				outputRowCount++;
			}
		}
	}
	int bytesCount=0;
	for(int c=0;c<columnCount;c++){
		//columnsInSelect[c]->columnLength=32;
		columnsInSelect[c]->columnOffset=bytesCount;
		bytesCount+=columnsInSelect[c]->columnLength+1;
	}

	return outputRowCount;
}

char** getTableData(tpd_entry* tab_entry, table_file_header* tfh){
	char* tname=(char*)calloc(1, MAX_TOK_LEN);
	memcpy(tname, tab_entry->table_name, MAX_TOK_LEN);
	strcat(tname, ".tab");
	FILE *fhandle = NULL;
	if((fhandle = fopen(tname, "rbc")) == NULL)
	{
		return NULL;
	}
	struct stat file_stat;
	fread(tfh, sizeof(table_file_header), 1, fhandle);
	fstat(fileno(fhandle), &file_stat);
	char* filedata = (char*)calloc(1, file_stat.st_size);

	if(IS_DEBUG) printf("num records - %d\nrecord length - %d\n",tfh->num_records, tfh->record_size);

	fread(filedata, file_stat.st_size, 1, fhandle);

	fflush(fhandle);
	fclose(fhandle);

	char** records=(char**) calloc(tfh->num_records, sizeof(char*));
	for (int i = 0; i < tfh->num_records; i++ ){
		records[i] = (char*) calloc(tfh->record_size, sizeof(char));
	}


	for(int i=0;i<tfh->num_records;i++){
		if(IS_DEBUG) printf("[sem_select] record size - %d\n", tfh->record_size);
		memcpy(records[i], filedata+i*tfh->record_size, tfh->record_size);
	}
	return records;
}

void printRowData(select_attribute** columnsInSelect, int columnCountInSelect, int rowCount, tpd_entry* tab_entry, bool* rowsToPrint, char** records){
	
	//int lenTillNow=0;
	for(int i=0;i<rowCount;i++){
		//lenTillNow=0;
		if(rowsToPrint!=NULL && !rowsToPrint[i]) continue;
		printCells(columnsInSelect, columnCountInSelect, i, tab_entry, records);
		printf("\n\t");
	}
}

void printCells(select_attribute** columnsInSelect, int columnCountInSelect, int rowNo, tpd_entry* tab_entry, char** records){
	cd_entry* col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
	for(int j = 0; j < columnCountInSelect; j++){

		if(columnsInSelect[j]->arr!=NULL){
			printCells(columnsInSelect[j]->arr, tab_entry->num_columns, rowNo, tab_entry, records);
			continue;
		}
		int colIndex=columnsInSelect[j]->columnIndex;
		int offset=columnsInSelect[j]->columnOffset;
		int colNameLen=strlen(columnsInSelect[j]->columnName);
		int cellLen = records[rowNo][offset];
		//printf("\n[sel_select] column detail %d %d %d %s\n", colIndex, offset, cellLen==0, columnsInSelect[j]->columnName);
		//printCharArrInInt(records[rowNo]+offset,5);
		offset++;
		if(cellLen==0){
			int len=0;
			if((col_entry+colIndex)->col_type==T_CHAR)len=colNameLen>(col_entry+colIndex)->col_len?colNameLen:(col_entry+colIndex)->col_len;
			else len=12;
			printf("% *s---% *s ",len/2-1,"",len/2-2,"");
		}else{
			char* cell = (char*)calloc(1, cellLen);
			//memcpy(cell, records[i][lenTillNow], cellLen);
			for(int k=0;k<cellLen;k++){
				*(cell+k)=records[rowNo][offset+k];
			}
			if((col_entry+colIndex)->col_type==T_CHAR || columnsInSelect[j]->functionType!=NONE)
				if(colNameLen>(col_entry+colIndex)->col_len)
					printf("%s% *s ", cell,colNameLen-strlen(cell), "");
				else
					printf("%s% *s ", cell, (col_entry+colIndex)->col_len-strlen(cell),"");
					
			else if((col_entry+colIndex)->col_type==T_INT){
				if(colNameLen>12)
					printf("% *s%d ", colNameLen-strlen(cell), "", bin2int(cell));
				else
					printf("%12d ", bin2int(cell));
			}else{

			}
		}
	}
}


void printColumnList(select_attribute** columnsInSelect, tpd_entry* tab_entry, int columnCountInSelect){
	cd_entry  *col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
	for(int i = 0; i < columnCountInSelect; i++){
		//printf("[sem_select] column detail %s\n", columnsInSelect[i]->columnName);
		int len=(col_entry+columnsInSelect[i]->columnIndex)->col_len;
		int index=columnsInSelect[i]->columnIndex;
		//printf("[printColumnList] %d %s\n", i, columnsInSelect[i]->columnName);
		if(columnsInSelect[i]->arr!=NULL){
			printColumnList(columnsInSelect[i]->arr, tab_entry, tab_entry->num_columns);
			continue;
		}
		//printf("\n<inside int cond %d, ind-%d>\n",(col_entry+index)->col_type, index);
		if((col_entry+columnsInSelect[i]->columnIndex)->col_type==T_INT){
			len=12;
		}

		char* columName=columnsInSelect[i]->columnName;
		if(columnsInSelect[i]->concatenatedName!=NULL){
			columName=columnsInSelect[i]->concatenatedName;
		}
		if(len>strlen(columName)){
			printf("%s", columName);
			printf("% *s ", len-strlen(columName),"");
		}else{
			printf("%s ", columName);
		}
		//printf("(%d,%d)",col_entry->col_len,strlen(col_entry->col_name));
	}
}

void printDashes(select_attribute** columnsInSelect, tpd_entry* tab_entry, int columnCountInSelect){
	cd_entry  *col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
	const char *pad = "---------------------------------------";

	for(int i = 0; i < columnCountInSelect; i++){
		//printf("[sem_select] printing ---\n");
		if(columnsInSelect[i]->arr!=NULL){
			printDashes(columnsInSelect[i]->arr, tab_entry, tab_entry->num_columns);
			continue;
		}
		int len=(col_entry+columnsInSelect[i]->columnIndex)->col_len;
		if((col_entry+columnsInSelect[i]->columnIndex)->col_type==T_INT) len=12;
		//printf("[sem_select] column len %d\n", len);
		if(strlen(columnsInSelect[i]->columnName)>len)
			printf("%.*s ", strlen(columnsInSelect[i]->columnName), pad);
		else
			printf("%.*s ", len, pad);
	}
}


int sem_delete_from(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	bool column_done = false;
	int cur_id = 0;
	cd_entry  *col_entry = NULL;
	cur = t_list;
	if(cur->tok_value!=K_FROM)
	{
		rc = INVALID_SYNTAX;
		cur->tok_value = INVALID;
	}
	else
	{
		cur = cur->next;
		if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
		{
			rc = TABLE_NOT_EXIST;
			cur->tok_value = INVALID;
		}
		else
		{
			FILE *fhandle = NULL;
			table_file_header* tfh = (table_file_header*)calloc(1, sizeof(table_file_header));
			char* tname=(char*)calloc(1, MAX_TOK_LEN);
			memcpy(tname, tab_entry->table_name, MAX_TOK_LEN);
			strcat(tname, ".tab");
			if((fhandle = fopen(tname, "rbc")) == NULL)
			{
				rc = FILE_OPEN_ERROR;
				if(IS_DEBUG) printf("file open issue");
			}
			else
			{
				struct stat file_stat;
				fread(tfh, sizeof(table_file_header), 1, fhandle);
				fstat(fileno(fhandle), &file_stat);
				char* filedata = (char*)calloc(1, file_stat.st_size);
				fread(filedata, file_stat.st_size, 1, fhandle);
				fclose(fhandle);
				char** records=(char**) calloc(tfh->num_records, sizeof(char*));
				bool* rowsToDelete=NULL;
				int offset=0;
				int numRowsDeleted=0;

				for (int i = 0; i < tfh->num_records; i++ ){
					records[i] = (char*) calloc(tfh->record_size, sizeof(char));
				}
				
				for(int i=0;i<tfh->num_records;i++){
					memcpy(records[i], filedata+i*tfh->record_size, tfh->record_size);
				}
				fflush(fhandle);
				fclose(fhandle);
				
				cur = cur->next;
				if(cur->tok_value == K_WHERE)
				{
					cur = cur->next;
					condition* conditionList = (condition*)calloc(1, 100);
					int conditionCount = parseWhereClause(cur, tab_entry, conditionList);
					if(conditionCount<0){
						rc = conditionCount;
						return rc;
					}
					rowsToDelete=filterRows(records, tab_entry, conditionList, conditionCount, tfh->num_records, tfh->record_size);
				}
				col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
				int cellLen=0;
				int numRowsRemaining=0;
				for(int i=0;i<tfh->num_records;i++){
					if(!rowsToDelete[i]){
						numRowsRemaining++;
					}
				}
				numRowsDeleted=tfh->num_records-numRowsRemaining;
				int rowNum=0;
				char** updatedRecords=(char**)calloc(numRowsRemaining, sizeof(char*));
				for(int i=0;i<tfh->num_records;i++){
					col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
					if(!rowsToDelete[i]){
						updatedRecords[rowNum] = (char*)calloc(tfh->record_size, sizeof(char));
						memcpy(updatedRecords[rowNum],records[i], tfh->record_size);
						rowNum++;
					}
				}
				int rc=0;
				if((fhandle = fopen(tname, "w")) == NULL)
				{
					rc = FILE_OPEN_ERROR;
					if(IS_DEBUG) printf("file open issue");
				}
				else
				{
					tfh->num_records=numRowsRemaining;
					fwrite(tfh, sizeof(table_file_header), 1, fhandle);
					fflush(fhandle);
					for(int i=0;i<numRowsRemaining;i++){
						fwrite(updatedRecords[i], tfh->record_size, 1, fhandle);
						fflush(fhandle);
					}
					fflush(fhandle);
					fclose(fhandle);
					printf("\n%d rows deleted\n", numRowsDeleted);
				}
			}
		}
	}
  return rc;
}


int sem_update(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	bool column_done = false;
	int cur_id = 0;
	cd_entry  *col_entry = NULL;
	cur = t_list;
	if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
	{
		rc = TABLE_NOT_EXIST;
		cur->tok_value = INVALID;
	}
	else
	{
		FILE *fhandle = NULL;
		table_file_header* tfh = (table_file_header*)calloc(1, sizeof(table_file_header));
		char* tname=(char*)calloc(1, MAX_TOK_LEN);
		memcpy(tname, tab_entry->table_name, MAX_TOK_LEN);
		strcat(tname, ".tab");
		if((fhandle = fopen(tname, "rbc")) == NULL)
		{
			rc = FILE_OPEN_ERROR;
		}
		else
		{
			cur=cur->next;
			struct stat file_stat;
			fread(tfh, sizeof(table_file_header), 1, fhandle);
			fstat(fileno(fhandle), &file_stat);
			char* filedata = (char*)calloc(1, file_stat.st_size);

			fread(filedata, file_stat.st_size, 1, fhandle);
			fclose(fhandle);

			char** records=(char**) calloc(tfh->num_records, sizeof(char*));
			bool* rowsToUpdate=NULL;
			int offset=0;
			int numRowsUpdated=0;


			for (int i = 0; i < tfh->num_records; i++ ){
				records[i] = (char*) calloc(tfh->record_size, sizeof(char));
			}
			
			
			for(int i=0;i<tfh->num_records;i++){
				memcpy(records[i], filedata+i*tfh->record_size, tfh->record_size);
			}
			fflush(fhandle);
			fclose(fhandle);
			int count=0;
			update_operation** columnListToUpdate = (update_operation**)calloc(100, sizeof(update_operation*));
			if(cur->tok_value==K_SET){
				cur=cur->next;
				count = parseSetClause(cur, tab_entry, columnListToUpdate);
				if(count<=0){
					return count;
				}
				printf("count- %d\n",count);
				while(cur->tok_value!=K_WHERE &&cur->tok_value!=EOC) cur=cur->next;
			}
			
			if(cur->tok_value == K_WHERE)
			{
				cur = cur->next;
				condition* conditionList = (condition*)calloc(1, 100);
				//printf("before condition parse - %s\n",cur->tok_string);
				int conditionCount = parseWhereClause(cur, tab_entry, conditionList);
				if(conditionCount<0){
					rc = conditionCount;
					return rc;
				}
				rowsToUpdate = filterRows(records, tab_entry, conditionList, conditionCount, tfh->num_records, tfh->record_size);

				while(cur->tok_value !=  EOC){
					cur=cur->next;
				}
			}
			//printf("before eoc %s\n",cur->tok_string);
			if(cur->tok_value != EOC){
				rc=INVALID_SYNTAX;
				cur->tok_value = INVALID;
				return rc;
			}
			numRowsUpdated=0;
			for(int u=0;u<count;u++){
				//printf("u-%d %d\n",u, columnListToUpdate[u]==NULL);
				int offset=getRowOffset(tab_entry, columnListToUpdate[u]->columnIndex);
				for(int i=0;i<tfh->num_records;i++){
					if(rowsToUpdate==NULL || rowsToUpdate[i]){
						//printf("columnListToUpdate[u]->columnIndex-%d\n",columnListToUpdate[u]->columnIndex);
						numRowsUpdated++;
						//printf("numRowsUpdated-%d\n",numRowsUpdated);
						int cellLen=*(records[i]+offset);
						//printf("cellLen-%d\n",cellLen);
						col_entry=(cd_entry*)((char*)tab_entry + tab_entry->cd_offset)+columnListToUpdate[u]->columnIndex;
						if(columnListToUpdate[u]->newValue==NULL){
							*(records[i]+offset)=0;
							memset(records[i]+offset, 0,cellLen);
						}
						else if(columnListToUpdate[u]->columnType==T_INT){
							*(records[i]+offset)=col_entry->col_len;
							int n = atoi(columnListToUpdate[u]->newValue);
							//printf("%d\n",n);
							*(records[i] + offset + 1 + 3) = (n >> 24) & 0xFF;
							*(records[i] + offset + 1 + 2) = (n >> 16) & 0xFF;
							*(records[i] + offset + 1 + 1) = (n >> 8) & 0xFF;
							*(records[i] + offset + 1 + 0) = n & 0xFF;
						}else{
							*(records[i]+offset)=col_entry->col_len;
							copyBytes(records[i] + offset + 1, columnListToUpdate[u]->newValue,cellLen);
						}
						//printf("cellLen-%d\n",cellLen);
					}
				}
			}
			//printf("\n%d before file save\n", numRowsUpdated);
			if((fhandle = fopen(tname, "w")) == NULL)
			{
				rc = FILE_OPEN_ERROR;
				if(IS_DEBUG) printf("file open issue");
			}
			else
			{
				fwrite(tfh, sizeof(table_file_header), 1, fhandle);
				fflush(fhandle);
				for(int i=0;i<tfh->num_records;i++){
					fwrite(records[i], tfh->record_size, 1, fhandle);
					fflush(fhandle);
				}
				fflush(fhandle);
				fclose(fhandle);
				printf("\n%d rows updated\n", numRowsUpdated);
			}
			
		}
	}
  return rc;
}

int bin2int(char* num){
	int intVal=0;
	*(num + 0)=*(num + 0)&0xff;
	*(num + 1)=*(num + 1)&0xff;
	*(num + 2)=*(num + 2)&0xff;
	*(num + 3)=*(num + 3)&0xff;
	//printCharArrInInt(num,4);
    memcpy(&intVal, num, sizeof(int));
	return intVal;
}

void copyIntToCharArray(char* arr, int val){
	*(arr + 3) = (val >> 24) & 0xFF;
	*(arr + 2) = (val >> 16) & 0xFF;
	*(arr + 1) = (val >> 8) & 0xFF;
	*(arr + 0) = val & 0xFF;
}

int columnExists(char* name, tpd_entry* tab_entry){
	cd_entry* col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
	for(int i = 0; i < tab_entry->num_columns; i++, col_entry++){
		if(strcmp(toLower(name), toLower(col_entry->col_name))==0){
			return i;
		}
	}
	return -1;
}

int getRowLen(select_attribute** columnList, int colLen){
	int len=0;
	for(int i=0;i<colLen;i++){
		len=len+columnList[i]->columnLength;
	}
	return len;
}

int getColumnList(token_list* cur, select_attribute** columnList){
	int columnCount=0;
	while(cur!=NULL && cur->tok_value!=K_FROM && cur->tok_value!=EOC){
		//printf("[getColumnList] %s %d\n",cur->tok_string, cur->tok_value);
		if((cur->tok_value==T_CHAR || cur->tok_value==S_STAR || cur->tok_value==IDENT) && (cur->next->tok_value==S_COMMA||cur->next->tok_value==K_FROM)){
			columnList[columnCount]=(select_attribute*)calloc(1,sizeof(select_attribute));
			memcpy(columnList[columnCount]->columnName, cur->tok_string, 32);
			columnList[columnCount]->functionType=NONE;
		}else if(cur->tok_value==F_SUM){
			columnList[columnCount]=(select_attribute*)calloc(1,sizeof(select_attribute));
			if(checkAggregate(SUM, cur, columnList[columnCount])!=0) return -1;
			cur=cur->next->next->next;// to move to next attribute, 'SUM' -> '(' -> 'column' -> ')'
		}
		else if(cur->tok_value==F_AVG){
			columnList[columnCount]=(select_attribute*)calloc(1,sizeof(select_attribute));
			if(checkAggregate(AVG, cur, columnList[columnCount])!=0) return -1;
			cur=cur->next->next->next;
		}
		else if(cur->tok_value==F_COUNT){
			columnList[columnCount]=(select_attribute*)calloc(1,sizeof(select_attribute));
			if(checkAggregate(COUNT, cur, columnList[columnCount])!=0) return -1;
			cur=cur->next->next->next;
		}
		else{
			cur->tok_value = INVALID;
			return INVALID_SELECT_SECTION;
		}
		cur=cur->next;
		if(cur->tok_value!=S_COMMA){
			if(cur->tok_value != K_FROM){
				cur->tok_value = INVALID;
				return INVALID_SELECT_SECTION;
			}else{
				columnCount++;
				break;
			}
		}
		cur=cur->next;
		columnCount++;
	}
	if(cur->tok_value==EOC){
		return INCOMPLETE_INSERT_STATEMENT;
	}
	//printf("[getColumnList] columnCount %d\n", columnCount);
	return columnCount;
}

int checkAggregate(aggregate_type aggType, token_list* cur, select_attribute* attr){
	attr->functionType=aggType;
	token_list* tempToken=cur;
	cur=cur->next;
	printf("[checkAggregate] %d\n",cur->tok_value);
	if(cur->tok_value==S_LEFT_PAREN ){
		cur=cur->next;
		if(cur->tok_value==T_CHAR || cur->tok_value==IDENT || cur->tok_value==S_STAR){
			memcpy(attr->columnName, cur->tok_string, strlen(cur->tok_string));
			cur=cur->next;
			if(cur->tok_value!=S_RIGHT_PAREN){
				cur->tok_value = INVALID;
				return INVALID_SELECT_SECTION;
			}
		}else{
			cur->tok_value = INVALID;
			return INVALID_SELECT_SECTION;
		}
	}else{
		cur->tok_value = INVALID;
		return INVALID_SELECT_SECTION;
	}
	attr->concatenatedName = (char*)calloc(40, sizeof(char));
	strcpy(attr->concatenatedName, tempToken->tok_string);
	strcat(attr->concatenatedName, tempToken->next->tok_string);
	strcat(attr->concatenatedName, tempToken->next->next->tok_string);
	strcat(attr->concatenatedName, tempToken->next->next->next->tok_string);
	return 0;
}


bool* filterRows(char** records, tpd_entry *tab_entry, condition* conditionList, int conditionCount, int rowCount, int rowLen){
	bool* filters=(bool*)calloc(1, rowCount);
	//printf("[filterRows] conditioncount %d, rowCount %d\n",conditionCount, rowCount);
	for(int i=0;i<rowCount;i++){
		filters[i]=true;
	}
	for(int i=0;i<conditionCount;i++){
		//printf("[filterRows] condition: %d, %s %d\n",conditionList[i].colNo,conditionList[i].data, conditionList[i].keywordLen);
		for(int r=0;r<rowCount;r++){
			if(filters[r] || (i>0 && conditionList[i-1].nextBinaryOperator==K_OR)){
				cd_entry  *col_entry = NULL;
				col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
				col_entry+=conditionList[i].colNo;
				//col_entry->col_len;
				//printf("getting offset %d\n",conditionList[i].colNo);
				int offset=getRowOffset(tab_entry, conditionList[i].colNo);
				//position at offset stores the length of the cell 
				bool isCellNull = *(records[r]+offset)==0;
				offset++;

				//printf("[filterRows] offset %d\n", offset);
				char* cell = NULL;
				if(!isCellNull){
					cell = (char*) calloc(32,sizeof(char));
					copyBytes(cell, records[r]+offset, col_entry->col_len);
				}
				//printCharArrInInt(cell, col_entry->col_len);
				//printf("cell-%s, condition-%s\n",cell,conditionList[i].data);
				bool result=false;
				if(conditionList[i].type==GREATER_THAN){
					if(cell==NULL){
						result=false;
					}
					else if(col_entry->col_type==T_INT){
						int value=bin2int(cell);
						//printf("\n[filterRows] value %d\n", value);
						result=value>stringToInt(conditionList[i].data);
					}
					else if(col_entry->col_type==T_CHAR){
						result=strcmp(cell, conditionList[i].data)>0;
					}
				}
				else if(conditionList[i].type==LESS_THAN){
					if(cell==NULL){
						result=false;
					}
					else if(col_entry->col_type==T_INT){
						int value=bin2int(cell);
						//printf("\n[filterRows] value %d\n", value);
						result=value<stringToInt(conditionList[i].data);
					}
					else if(col_entry->col_type==T_CHAR){
						result=strcmp(cell, conditionList[i].data)<0;
					}
				}
				else if(conditionList[i].type==EQUALS){
					if(cell==NULL){
						result=false;
					}
					else if(col_entry->col_type==T_INT){
						int value=bin2int(cell);
						//printf("\n[filterRows] value %d\n", value);
						result=value==stringToInt(conditionList[i].data);
					}
					else if(col_entry->col_type==T_CHAR){
						result=strcmp(cell, conditionList[i].data)==0;
					}
				}
				else if(conditionList[i].type==NOT_EQUALS){
					if(cell==NULL){
						result=false;
					}
					else if(col_entry->col_type==T_INT){
						int value=bin2int(cell);
						//printf("\n[filterRows] value %d\n", value);
						result=value!=stringToInt(conditionList[i].data);
					}
					else if(col_entry->col_type==T_CHAR){
						result=strcmp(cell, conditionList[i].data)!=0;
					}
				}
				else if(conditionList[i].type==IS_NOT_NULL){
					result=!isCellNull;
				}
				else if(conditionList[i].type==IS_NULL){
					result=isCellNull;
				}

				if(i>0){
					if(conditionList[i-1].nextBinaryOperator==K_AND){
						filters[r]=filters[r] && result;
					}
					else if(conditionList[i-1].nextBinaryOperator==K_OR){
						filters[r]=filters[r] || result;
					}
				}else
					filters[r]=result;
			}
		}
	}
	return filters;
}

int filterColumns_bk(select_attribute** attributes, int attributeCount, tpd_entry *tab_entry, select_attribute** filters){
	cd_entry* col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
	int attributeNo=0;
	for(int att=0;att<attributeCount;att++){
		if(strlen(attributes[att]->columnName)==1 && attributes[att]->columnName[0]=='*'){
			attributes[att]->columnLength=4;//if there is * in select clause, its output for aggregates can only be an integer
			cd_entry* col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
			for(int i = 0; i < tab_entry->num_columns; i++, col_entry++){
				filters[attributeNo]=(select_attribute*)calloc(1, sizeof(select_attribute));
				filters[attributeNo]->columnIndex=i;
				filters[attributeNo]->columnOffset=getRowOffset(tab_entry, i);
				filters[attributeNo]->functionType = attributes[att]->functionType;
				cd_entry  *col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
				filters[attributeNo]->columnLength = col_entry[i].col_len;
				memcpy(filters[attributeNo]->columnName,col_entry->col_name, strlen(col_entry->col_name));
				//printf("[filterColumns] %s %d, offset-%d\n", filters[attributeNo]->columnName, filters[attributeNo]->columnIndex, filters[attributeNo]->columnOffset);
				attributeNo++;
			}
		} else {
			cd_entry* col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
			bool foundColumn=false;
			for(int i = 0; i < tab_entry->num_columns; i++, col_entry++){
				//printf("[filterColumns] %s %s\n", col_entry->col_name, attributes[att]->columnName);
				if(strcmp(toLower(col_entry->col_name),toLower(attributes[att]->columnName))==0){
					filters[attributeNo]=(select_attribute*)calloc(1, sizeof(select_attribute));
					filters[attributeNo]->columnIndex=i;
					filters[attributeNo]->columnOffset=getRowOffset(tab_entry, i);
					filters[attributeNo]->functionType = attributes[att]->functionType;
					cd_entry  *col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
					filters[attributeNo]->columnLength = col_entry[i].col_len;
					attributes[att]->columnLength=col_entry[i].col_len;
					attributes[att]->columnOffset=filters[attributeNo]->columnOffset;
					attributes[att]->columnIndex=i;
					if((attributes[att]->functionType==SUM||attributes[att]->functionType==AVG)&&col_entry->col_type==T_CHAR){
						return INVALID_AGGREGATE_FUNCTION;
					}
					memcpy(filters[attributeNo]->columnName,col_entry->col_name, strlen(col_entry->col_name));
					foundColumn=true;
					attributeNo++;
					break;
				}
			}
			if(!foundColumn)
				return COLUMN_NOT_EXIST;
			
		}

		

	}
	return attributeNo;
}

int filterColumns(select_attribute** attributes, int attributeCount, tpd_entry *tab_entry){
	cd_entry* col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
	int attributeNo=0;
	for(int att=0;att<attributeCount;att++){
		//printf("[filterColumns] %s \n",attributes[att]->columnName);
		if(strlen(attributes[att]->columnName)==1 && attributes[att]->columnName[0]=='*'){
			attributes[att]->columnLength=4;//if there is * in select clause, its output for aggregates can only be an integer
			col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
			select_attribute** filters = (select_attribute**)calloc(100, sizeof(select_attribute*));
			for(int i = 0; i < tab_entry->num_columns; i++, col_entry++){
				filters[i]=(select_attribute*)calloc(1, sizeof(select_attribute));
				filters[i]->columnIndex=i;
				filters[i]->columnOffset=getRowOffset(tab_entry, i);
				filters[i]->functionType = attributes[att]->functionType;
				cd_entry  *col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
				filters[i]->columnLength = col_entry[i].col_len;
				memcpy(filters[i]->columnName,col_entry[i].col_name, strlen(col_entry->col_name));
				memcpy(filters[i]->tableName,tab_entry->table_name, strlen(tab_entry->table_name));				
				//printf("[filterColumns] %s %d, offset-%d\n", filters[attributeNo]->columnName, filters[attributeNo]->columnIndex, filters[attributeNo]->columnOffset);
			}
			attributes[att]->arr=filters;
		} else {
			col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
			bool foundColumn=false;
			for(int i = 0; i < tab_entry->num_columns; i++, col_entry++){
				//printf("[filterColumns] %s %s\n", col_entry->col_name, attributes[att]->columnName);
				if(strcmp(toLower(col_entry->col_name),toLower(attributes[att]->columnName))==0){
					//cd_entry  *col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
					attributes[att]->columnLength=col_entry->col_len;
					attributes[att]->columnOffset=getRowOffset(tab_entry, i);
					attributes[att]->columnIndex=i;
					memcpy(attributes[att]->tableName,tab_entry->table_name, strlen(tab_entry->table_name));				
					printf("[filterColumns] - tablename %s, col_len %d\n", tab_entry->table_name, col_entry->col_len);
					//printf("%s %d aggregate func - %d\n", col_entry->col_name, col_entry->col_type, attributes[att]->functionType);
					if((attributes[att]->functionType==SUM||attributes[att]->functionType==AVG)&&col_entry->col_type==T_CHAR){
						return INVALID_AGGREGATE_FUNCTION;
					}
					foundColumn=true;
					break;
				}
			}
			// if(!foundColumn)
			// 	return COLUMN_NOT_EXIST;
			
		}

		

	}
	//printf("[filterColumns] complete\n");
	return 0;
}


int getRowOffset(tpd_entry *tab_entry, int colNo){
	cd_entry  *col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
	int offset=0;
	for(int i = 0; i < tab_entry->num_columns; i++, col_entry++){
		if(i==colNo) break;
		offset++;
		offset+=col_entry->col_len;
	}
	return offset;
}

int parseSetClause(token_list* cur, tpd_entry* tab_entry, update_operation** columnListToUpdate){
	int operationNo=0;
	while(cur->next!=NULL){
		//printf("[parseSetClause] %s, %d, %d\n",cur->tok_string, cur->tok_value, T_CHAR);
		if(cur->tok_value==T_CHAR || cur->tok_value==IDENT){
			bool contains=false;
			cd_entry* col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
			for(int i = 0; i < tab_entry->num_columns; i++, col_entry++){
				//printf("[parseWhereClause] comparing - %s %s\n", toLower(col_entry->col_name), toLower(cur->tok_string));
				if(strcmp(toLower(col_entry->col_name),toLower(cur->tok_string))==0){
					contains=true;
					columnListToUpdate[operationNo] = (update_operation*)calloc(100, sizeof(update_operation));
					columnListToUpdate[operationNo]->columnIndex=i;
					columnListToUpdate[operationNo]->columnType=col_entry->col_type;
					cur=cur->next;
					if(cur->tok_value!=S_EQUAL){
						cur->tok_value = INVALID;
						return INVALID_UPDATE_STATEMENT;
					}else{
						cur=cur->next;
						//printf("%d %d (%d)\n", cur->tok_value , col_entry->col_type, operationNo);
						if(cur->tok_value==INT_LITERAL && col_entry->col_type==T_INT || cur->tok_value==STRING_LITERAL && col_entry->col_type==T_CHAR){
							columnListToUpdate[operationNo]->newValue = (char*)calloc(32, sizeof(char));
							memcpy(columnListToUpdate[operationNo]->newValue, cur->tok_string, 32);
							operationNo++;
							cur=cur->next;
							contains=true;
							break;
						}else if(cur->tok_value==K_NULL){
							//printf("bool - %d\n", col_entry->not_null);
							if(col_entry->not_null){
								cur->tok_value = INVALID;
								return NULL_NOT_ALLOWED;
							}else{
								columnListToUpdate[operationNo]->newValue = NULL;
								operationNo++;
								cur=cur->next;
								contains=true;
								break;
							}
						}else{
							cur->tok_value = INVALID;
							return DATA_TYPE_MISMATCH;
						}
					}
					//printf("\n\n[parseSetClause] col no. - %d\n", columnListToUpdate[operationNo]->columnIndex);
					break;
				}
			}
			if(!contains){
				cur->tok_value = INVALID;
				return COLUMN_NOT_EXIST;
			}
			//printf("\n[parseSetClause] next word-%s\n",cur->tok_string);
			if(cur->tok_value==EOC || cur->tok_value==K_WHERE) break;
			if(cur->tok_value==S_COMMA){
			}else{
				cur->tok_value = INVALID;
				return INVALID_SYNTAX;
			}
			//printf("\n[parseSetClause] condition: %d, %s %d\n",columnListToUpdate[operationNo]->columnIndex,columnListToUpdate[operationNo]->columnName, columnListToUpdate[operationNo]->newValue);
	cur=cur->next;
			//operationNo++;
		}else{
			cur->tok_value = INVALID;
			return INVALID_SYNTAX;
		}
	}
	printf("\n[parseSetClause] operationNo-%d\n",operationNo);
	return operationNo==0?INVALID_UPDATE_STATEMENT:operationNo;
}

int parseWhereClause(token_list* cur, tpd_entry* tab_entry, condition* conditionList){
	int conditionNo=0;
	cd_entry  *col_entry = NULL;
	col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
	while(cur->next!=NULL){
		//printf("[parseWhereClause] reading condition\n");
		//printf("[parseWhereClause] %s, %d, %d\n",cur->tok_string, cur->tok_value, T_CHAR);
		if(cur->tok_value==T_CHAR || cur->tok_value==IDENT){
			bool contains=false;
			col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
			for(int i = 0; i < tab_entry->num_columns; i++, col_entry++){
				//printf("[parseWhereClause] comparing - %s %s\n", toLower(col_entry->col_name), toLower(cur->tok_string));
				if(strcmp(toLower(col_entry->col_name),toLower(cur->tok_string))==0){
					contains=true;
					conditionList[conditionNo] = *parseCondition(cur->next, col_entry->col_type);
					conditionList[conditionNo].colNo=i;
					if(conditionList[conditionNo].type==-1){
						cur->tok_value = INVALID;
						return -INVALID_COLUMN_NAME;
					}
					while(conditionList[conditionNo].keywordLen>=0){
						cur=cur->next;
						conditionList[conditionNo].keywordLen--;
					}
					//printf("\n\n[parseWhereClause] col no. - %d\n", conditionList[conditionNo].colNo);
					break;
				}
			}
			if(!contains){
				cur->tok_value = INVALID;
				return COLUMN_NOT_EXIST;
			}
			//printf("\n[parseWhereClause] next word-%s\n",cur->tok_string);
			if(cur->tok_value==EOC || cur->tok_value==K_ORDER) break;
			if(cur->tok_value==K_AND){
				conditionList[conditionNo].nextBinaryOperator=K_AND;
			}else if(cur->tok_value==K_OR){
				conditionList[conditionNo].nextBinaryOperator=K_OR;
			}else{
				cur->tok_value = INVALID;
				return INVALID_SYNTAX;
			}
			//printf("\n[parseWhereClause] condition: %d, %s %d\n",conditionList[conditionNo].colNo,conditionList[conditionNo].data, conditionList[conditionNo].keywordLen);
	cur=cur->next;
			conditionNo++;
		}else{
			cur->tok_value = INVALID;
			return INVALID_SYNTAX;
		}
	}
	//printf("\n[parseWhereClause] final count: %d\n",conditionNo+1);
	return conditionNo+1;
}

condition* parseCondition(token_list *t_list, int colType){
	token_list *cur=t_list;
	condition* c = (condition*) calloc(1,sizeof(condition));
	c->type=INVALID_CONDITION;
	c->keywordLen=0;
	if(cur->tok_value==K_IS){
		cur=cur->next;
		c->keywordLen++;
		if(cur->tok_value==K_NOT){
			cur=cur->next;
			c->keywordLen++;
			if(cur->tok_value==K_NULL){
				c->type=IS_NOT_NULL;
				c->keywordLen++;
			}
		}
		else if(cur->tok_value==K_NULL){
			c->keywordLen++;
			c->type = IS_NULL;
		}
	}else{ 
		c->keywordLen++;
		if(cur->tok_value==S_EQUAL){
			c->type=EQUALS;
		}else if(cur->tok_value==S_LESS){
			c->type=LESS_THAN;
		}else if(cur->tok_value==S_GREATER){
			c->type=GREATER_THAN;
		}
		//printf("\n[parseCondition] type-%d\n", c->type);
		cur=cur->next;
		c->keywordLen++;
		//printf("\n[parseCondition] datatype - %d, %d\n", cur->tok_value, colType);
		if(c->type!=-1){
			if(cur->tok_value==INT_LITERAL && colType==T_INT || cur->tok_value==STRING_LITERAL && colType==T_CHAR)
				strcpy(c->data, cur->tok_string);
			else
				c->type=INVALID_CONDITION;
		}
	}
	return c;
}

int stringToInt(char arr[]){
	int len=strlen(arr);
	int res=0;
	for(int i=0;i<len;i++){
		res=res*10+(arr[i]-'0');
	}
	return res;
}

char* toLower(char* s){
	char* arr=(char*)calloc(strlen(s)+1, 1);
	for(int i=0;s[i]!='\0';i++){
		if(s[i]<91) arr[i]=s[i]+32;
		else arr[i]=s[i];
	}
	return arr;
}

void printCharArr(char arr[], int size){
	for(int i=0;i<size;i++)
	{
		printf("(%d)%c",i, arr[i]);
		//printf("%c", arr[i]);
	}
	printf("\n<end>\n");
}

void printCharArrInInt(char arr[], int size){
	printf("\n");
	for(int i=0;i<size;i++)
	{
		printf("%d ",arr[i]);
	}
	printf("\n");
}


void copyBytes(char* from, char to[], int noOfBytes){
	for(int i=0;i<noOfBytes;i++){
		*(from+i)=to[i];
		//printf("copying - %d\n",*(from+i));
	}
	if(IS_DEBUG) printf("copy - %s\n",from);
}


int initialize_tpd_list()
{
	int rc = 0;
	FILE *fhandle = NULL;
//	struct _stat file_stat;
	struct stat file_stat;

  /* Open for read */
  if((fhandle = fopen("dbfile.bin", "rbc")) == NULL)
	{
		if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
		{
			rc = FILE_OPEN_ERROR;
		}
		else
		{
			g_tpd_list = NULL;
			g_tpd_list = (tpd_list*)calloc(1, sizeof(tpd_list));
			
			if (!g_tpd_list)
			{
				rc = MEMORY_ERROR;
			}
			else
			{
				g_tpd_list->list_size = sizeof(tpd_list);
				fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
				fflush(fhandle);
				fclose(fhandle);
			}
		}
	}
	else
	{
		/* There is a valid dbfile.bin file - get file size */
//		_fstat(_fileno(fhandle), &file_stat);
		fstat(fileno(fhandle), &file_stat);
		printf("dbfile.bin size = %d\n", file_stat.st_size);

		g_tpd_list = (tpd_list*)calloc(1, file_stat.st_size);

		if (!g_tpd_list)
		{
			rc = MEMORY_ERROR;
		}
		else
		{
			fread(g_tpd_list, file_stat.st_size, 1, fhandle);
			fflush(fhandle);
			fclose(fhandle);

			if (g_tpd_list->list_size != file_stat.st_size)
			{
				rc = DBFILE_CORRUPTION;
			}

		}
	}
    
	return rc;
}
	
int add_tpd_to_list(tpd_entry *tpd)
{
	int rc = 0;
	int old_size = 0;
	FILE *fhandle = NULL;

	if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
  else
	{
		old_size = g_tpd_list->list_size;

		if (g_tpd_list->num_tables == 0)
		{
			/* If this is an empty list, overlap the dummy header */
			g_tpd_list->num_tables++;
		 	g_tpd_list->list_size += (tpd->tpd_size - sizeof(tpd_entry));
			fwrite(g_tpd_list, old_size - sizeof(tpd_entry), 1, fhandle);
		}
		else
		{
			/* There is at least 1, just append at the end */
			g_tpd_list->num_tables++;
		 	g_tpd_list->list_size += tpd->tpd_size;
			fwrite(g_tpd_list, old_size, 1, fhandle);
		}

		fwrite(tpd, tpd->tpd_size, 1, fhandle);
		fflush(fhandle);
		fclose(fhandle);
	}

	return rc;
}

int drop_tpd_from_list(char *tabname)
{
	int rc = 0;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;
	int count = 0;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (strcasecmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				int old_size = 0;
				FILE *fhandle = NULL;

				if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
				{
					rc = FILE_OPEN_ERROR;
				}
			  else
				{
					old_size = g_tpd_list->list_size;

					if (count == 0)
					{
						/* If this is the first entry */
						g_tpd_list->num_tables--;

						if (g_tpd_list->num_tables == 0)
						{
							/* This is the last table, null out dummy header */
							memset((void*)g_tpd_list, '\0', sizeof(tpd_list));
							g_tpd_list->list_size = sizeof(tpd_list);
							fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
						}
						else
						{
							/* First in list, but not the last one */
							g_tpd_list->list_size -= cur->tpd_size;

							/* First, write the 8 byte header */
							fwrite(g_tpd_list, sizeof(tpd_list) - sizeof(tpd_entry),
								     1, fhandle);

							/* Now write everything starting after the cur entry */
							fwrite((char*)cur + cur->tpd_size,
								     old_size - cur->tpd_size -
										 (sizeof(tpd_list) - sizeof(tpd_entry)),
								     1, fhandle);
						}
					}
					else
					{
						/* This is NOT the first entry - count > 0 */
						g_tpd_list->num_tables--;
					 	g_tpd_list->list_size -= cur->tpd_size;

						/* First, write everything from beginning to cur */
						fwrite(g_tpd_list, ((char*)cur - (char*)g_tpd_list),
									 1, fhandle);

						/* Check if cur is the last entry. Note that g_tdp_list->list_size
						   has already subtracted the cur->tpd_size, therefore it will
						   point to the start of cur if cur was the last entry */
						if ((char*)g_tpd_list + g_tpd_list->list_size == (char*)cur)
						{
							/* If true, nothing else to write */
						}
						else
						{
							/* NOT the last entry, copy everything from the beginning of the
							   next entry which is (cur + cur->tpd_size) and the remaining size */
							fwrite((char*)cur + cur->tpd_size,
										 old_size - cur->tpd_size -
										 ((char*)cur - (char*)g_tpd_list),							     
								     1, fhandle);
						}
					}

					fflush(fhandle);
					fclose(fhandle);
					if( remove( strcat(cur->table_name, ".tab") ) != 0 )
						rc=TABLE_NOT_EXIST;
					
					
				}

				
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
					count++;
				}
			}
		}
	}
	
	if (!found)
	{
		rc = INVALID_TABLE_NAME;
	}

	return rc;
}

tpd_entry* get_tpd_from_list(char *tabname)
{
	tpd_entry *tpd = NULL;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (strcasecmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				tpd = cur;
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
				}
			}
		}
	}

	return tpd;
}