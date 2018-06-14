#include<iostream>
#include<cstdio>
#include<stdio.h> 
#include<unistd.h>
#include<sys/types.h>
#include<errno.h>
#include <stdlib.h>
#include<string.h>
#include<signal.h>
#include <fcntl.h>
#include<set>

using namespace std;

char sqlprompt[100];
int gflag=0;
char table[20][500];
char *columns[20];
char *tables[20];
char *where[20];
int tablelen[10];
int num_columns;
int num_where;
int num_tables;
int whereflag=0;
int gflag1=0;
int disflag=0;
int writeflag=0;
int fileindex;

void  parse(char *line, char **argv)
{
	while (*line != '\0') {     
		while (*line == ' ' || *line == '\t' || *line == '\n')
			*line++ = '\0';     
		*argv++ = line;          
		while (*line != '\0' && *line != ' ' && 
				*line != '\t' && *line != '\n') 
			line++;             
	}
	*argv = '\0';                 
}



void getrow(char row[],int fd1){

	while(1){
		char buf;
		int actual_count = read(fd1, &buf, 1);
		if(actual_count<=0)
			gflag=1;
		if(buf!='"')
			strncat(row, &buf, 1);
		if(buf=='\n')
			break;
	}
}


void  parserow(char *line, char **argv)
{
	while (*line != '\0') {     
		while (*line == ',' || *line=='\n')
			*line++ = '\0';     
		*argv++ = line;          
		while (*line != '\0' && *line != ',' && 
				*line != '\n') 
			line++;             
	}
	*argv = '\0';     
}

void getrow1(char row[],int fd2){

	while(1){
		char buf;
		int actual_count = read(fd2, &buf, 1);
		if(actual_count<=0)
			gflag1=1;
		if(buf!='"')
			strncat(row, &buf, 1);
		if(buf=='\n')
			break;
	}
}

int main(int argc, char *argv[])
{
	strcpy(sqlprompt,"megatron_sql_engine");

	int fd1,i;
	char c;
	char tmp[100]="";
	fd1 = open("schema", O_RDONLY);
	int len=0,sublen=0;
	int tablenum=0;

	while(1){
		if(gflag==1)
			break;
		char row[500]="";
		getrow(row,fd1);
		if(strcmp(row,"\n")==0)
		{
			tablelen[tablenum++]=sublen;
			sublen=0;
		}
		//	printf("%s\n",row);
		if(strcmp(row,"\n")!=0)
		{
			strcpy(table[len],row);
			len++;
			sublen++;
		}
	}
	//printf("%d\n",tablelen[0]);
	close(fd1);
	gflag=0;

	//	for(i=0;i<len;i++)
	//		printf("%s\n",table[i]);
	//	for(i=0;i<tablelen[0];i++)
	//		printf("%s\n",table[i]);
	//	for(i=0;i<tablelen[1];i++)
	//		printf("%s\n",table[tablelen[0]+i]);
	printf("<%s> ",sqlprompt);

	while(1)
	{
		c = getchar();
		if(c==EOF)
			continue;
		if(c=='\n'){
			if (strcmp(tmp,"")==0)
			{
				printf("<%s> ",sqlprompt);
				continue;
			}
			char *argv[200];
			parse(tmp,argv);
			int len1=0;
			while(argv[len1]!=NULL)
				len1++;
			if(len1<=3)
				printf("Invalid query\n");
			else
			{
				if(strcmp(argv[len1-2],"|")==0)
				{
					writeflag=1;
					fileindex=len1-1;
					len1=len1-2;
				}

				char discol[200];

				if(argv[1][0]=='d' && argv[1][1]=='i' && argv[1][2]=='s')
				{
					disflag=1;
					char *apch;
					char yy[200];
					strcpy(yy,argv[1]);
					apch = strtok (yy,"()");

					while (apch != NULL)
					{
						//printf ("%s\n",apch);
						apch = strtok (NULL, "()");
						strcpy(discol,apch);
						//printf ("%s\n",discol);
						break;
					}
				}


				int i,j=0,index;
				char *sch;
				for(i=1;i<len1;i++){
					if(!strcmp(argv[i],"from")){
						index=i;
						break;
					}
					else{
						if(disflag==1)
							sch=strdup(discol);
						else
							sch=strdup(argv[i]);
					}
				}
				char *pch;j=0;
				pch = strtok (sch,",");
				while (pch != NULL)
				{
					//rows++;
					columns[j++]=strdup(pch);
					pch = strtok (NULL, ",");
				}
				num_columns=j;
				for(i=index+1;i<len1;i++){
					if(!strcmp(argv[i],"where")){
						index=i;
						whereflag=1;
						break;
					}
					else
						sch=strdup(argv[i]);
				}
				j=0;
				pch = strtok (sch,",");
				while (pch != NULL)
				{
					//rows++;
					tables[j++]=strdup(pch);
					pch = strtok (NULL, ",");
				}
				num_tables=j;
				//cout<<index<<endl;
				if(whereflag==1)
				{
					j=0;
					for(i=index+1;i<len1;i++){
						//	cout<<argv[i]<<endl;
						where[j++]=strdup(argv[i]);
					}
					/*
					   bzero(sch,sizeof(sch));
					   for(i=index+1;i<len1;i++){
					   sch=strdup(argv[i]);
					   }
					   j=0;
					   pch = strtok (sch," ");
					   while (pch != NULL)
					   {
					//rows++;
					where[j++]=strdup(pch);
					pch = strtok (NULL, " ");
					}*/
					num_where=j;
				}
				//cout<<"hi   "<<num_where<<endl;

		/*		for(i=0;i<num_columns;i++)
					cout<<columns[i]<<endl;
				for(i=0;i<num_tables;i++)
					cout<<tables[i]<<endl;
				for(i=0;i<num_where;i++)
					cout<<where[i]<<endl;
				cout<<endl;
*/
				int startlen=0;
				int no=len;

				char file[20];
				int tabflag=0;
				char givenfile[20];
				strcpy(givenfile,argv[3]);
				strncat(givenfile,"\n",1);

				strcpy(file,argv[3]);
				strncat(file,".csv",4);
				int chktable=0;
				int tabnum=0;
				char joinfile1[20];
				char joinfile2[20];
				for(i=0;i<len;i++)
				{
					//	cout<<table[i]<<endl;
					if(strcmp(table[i],givenfile)==0)
					{
						chktable=1;
						startlen=i;
						//no=tablelen[tabnum-1]-1;
						break;
					}
					tabnum=(int)table[i][0]-48;
				}
				//cout<<"num is "<<tabnum<<endl;
				//cout<<"num of tables"<<num_tables<<endl;
				int tabnum1,tabnum2;
				if(num_tables==2)
				{
					char giventab[20];
					strcpy(giventab,tables[0]);
					strncat(giventab,"\n",1);
					char giventab1[20];
					strcpy(giventab1,tables[1]);
					strncat(giventab1,"\n",1);
					//cout<<giventab<<endl;
					for(i=0;i<len;i++)
					{
						//cout<<table[i]<<endl;
						if(strcmp(table[i],giventab)==0)
						{
							chktable=1;
							//startlen=i;
							//no=tablelen[tabnum-1]-1;
							break;
						}
						tabnum1=(int)table[i][0]-48;
					}
					chktable=0;
					for(i=0;i<len;i++)
					{
						//cout<<table[i]<<endl;
						if(strcmp(table[i],giventab1)==0)
						{
							chktable=1;
							//startlen=i;
							//no=tablelen[tabnum-1]-1;
							break;
						}
						tabnum2=(int)table[i][0]-48;
					}
					no=len;
					tabflag=3;
					strcpy(joinfile1,tables[0]);
					strncat(joinfile1,".csv",4);
					strcpy(joinfile2,tables[1]);
					strncat(joinfile2,".csv",4);
				}
				






				int colnum[20];
				int wnum;
				char *type;
				int wnum1;
				char *type1;
				int belongto[20];
				int chkflag=0;
				int wflag=0;
				int wflag1=0;
				int match=0;
				if(whereflag==1)
				{
					int colflag=0;
					for(i=startlen;i<no;i++){
						char *pch;
						char sr1[100];
						strcpy(sr1,table[i]);
						pch = strtok (sr1,",");
						wnum=(int)pch[0]-48;
						while (pch != NULL)
						{
							if(strcmp(where[0],pch)==0){
								//cout<<"ki"<<endl;
								wflag=1;
								colflag=1;
								chkflag=1;
							}
							pch = strtok (NULL, " ,.-");
							if(wflag==1)
							{
								type=strdup(pch);
								break;
							}

						}
						if(colflag==1)
							break;
					}

					int colflag1=0;
					for(i=startlen;i<no;i++){
						char *pch;
						char sr1[100];
						strcpy(sr1,table[i]);
						pch = strtok (sr1,",");
						wnum1=(int)pch[0]-48;
						while (pch != NULL)
						{
							if(strcmp(where[2],pch)==0){
								//cout<<"ki"<<endl;
								wflag1=1;
								match=1;
								colflag1=1;
								chkflag=1;
							}
							pch = strtok (NULL, " ,.-");
							if(wflag1==1)
							{
								type1=strdup(pch);
								break;
							}

						}
						if(colflag1==1)
							break;
					}
				}
/*				cout<<"type is   "<<type<<endl;
				cout<<"wnum is  "<<wnum<<endl;
				cout<<"type1 is   "<<type1<<endl;
				cout<<"wnum1 is  "<<wnum1<<endl;
				cout<<"match is  "<<match<<endl;


*/

				for(j=0;j<num_columns;j++)
				{
					chkflag=0;
					int colflag=0;
					for(i=startlen;i<no;i++){
						char *pch;
						char *copy;
						char sr[100];
						strcpy(sr,table[i]);
						pch = strtok (sr,",");
						while (pch != NULL)
						{
							if(strcmp(columns[j],pch)==0){
								if(tabflag==3 && i<tablelen[0])
									belongto[j]=0;
								else
									belongto[j]=1;
								chkflag=1;
								//	cout<<copy<<endl;
								colnum[j]=(int)copy[0]-48;
								//printf("%d\n",colnum[j]);
								colflag=1;
								break;
							}
							//s	printf ("%s\n",pch);
							copy=strdup(pch);
							pch = strtok (NULL, " ,.-");
						}
						if(colflag==1)
							break;
					}
				}

			//	cout<<belongto[0]<<endl;
			//	cout<<belongto[1]<<endl;
				int fd7;
				if(writeflag==1){
				FILE *log = fopen(argv[fileindex],"w");
				fclose(log);
				fd7 = open(argv[fileindex],O_WRONLY);
				//cout<<argv[fileindex]<<endl;
				}


				if(strcmp(argv[1],"*")==0)
				{
					chkflag=1;
					//cout<<tablelen[tabnum-1]<<endl;

						for(i=0;i<tablelen[tabnum-1];i++)
							colnum[i]=i+1;
						num_columns=tablelen[tabnum-1]-2;
					//cout<<"col "<<num_columns<<endl;
/*
					if(tabflag==1)
					{
						for(i=0;i<tablelen[0];i++)
							colnum[i]=i+1;
						num_columns=tablelen[0]-1;
					}
					else if(tabflag==2)
					{
						for(i=0;i<tablelen[1];i++)
							colnum[i]=i+1;
						num_columns=tablelen[1]-1;
					}*/
					if(tabflag==3)
					{
						for(i=0;i<tablelen[tabnum1-1]-2;i++)
						{
							colnum[i]=i+1;
							belongto[i]=0;
						}
						int g=0;
						for(i=tablelen[tabnum1-1]-2;i<tablelen[tabnum1-1]+tablelen[tabnum2-1]-4;i++)
						{
							colnum[i]=g+1;
							belongto[i]=1;
							g++;
						}
						num_columns=tablelen[tabnum1-1]+tablelen[tabnum2-1]-4;
						//cout<<"both "<<num_columns<<endl;

					}
				}

				if(tabflag==3 && chkflag==1 && chktable==1){
					set <string> myarr;
					int fd3,n;	
					char store[10][500];
					fd3 = open(joinfile2, O_RDONLY);
					char c1;
					char str[10000];
					char *tempfile[100000];
					int filecnt=0;
					while(n = read(fd3, &c1, 1) > 0) {
						if(c1=='\n'){
							int k;
							char roww[10000];
							strcpy(roww,"");
							int m=0,n=0;
							for(k=0;k<strlen(str);k++){
								if(str[k]==',')
								{
									store[m++][n]='\0';
									n=0;
									continue;
								}
								if(str[k]!='\n' && str[k]!='"')
									store[m][n++]=str[k];
							}
							store[m][n]='\0';
							for(k=0;k<num_columns-1;k++)
							{
								if(belongto[k]==1){
									//	printf("%s ",store[colnum[k]-1]);
									strncat(roww,store[colnum[k]-1],200);
									strncat(roww," ",200);
								}
							}
							if(disflag==1)
							{
								if(belongto[k]==1){
									if(myarr.count(store[colnum[k]-1])){}
									else{
										myarr.insert(store[colnum[k]-1]);
										//printf("%s\n",store[colnum[k]-1]);
										strncat(roww,store[colnum[k]-1],200);
									}
								}
							}
							else{
								if(belongto[k]==1){
								//	cout<<k<<endl;
									//	printf("%s\n",store[colnum[k]-1]);
									strncat(roww,store[colnum[k]-1],200);
								}
							}
							//if(belongto[k]==1)
							tempfile[filecnt++]=strdup(roww);
							strcpy(str,"");
						}

						else if(c=='"')continue;
						else
							strncat(str,&c1,1);
					}
					close(fd3);
					//for(i=0;i<filecnt;i++)
					//	printf("%s\n",tempfile[i]);


					set <string> myarr1;
					int fd4,n1;	
					char store1[10][500];
					fd4 = open(joinfile1, O_RDONLY);
					char c2;
					char str1[10000];
					while(n1 = read(fd4, &c2, 1) > 0) {
						if(c2=='\n'){
							int k;
							int m=0,n=0;
							for(k=0;k<strlen(str1);k++){
								if(str1[k]==',')
								{
									store1[m++][n]='\0';
									n=0;
									continue;
								}
								if(str1[k]!='\n' && str1[k]!='"')
									store1[m][n++]=str1[k];
							}
							store1[m][n]='\0';
							for(i=0;i<filecnt;i++)
							{
								int m1=0,n1=0;
								char store2[20][400];
								for(k=0;k<strlen(tempfile[i]);k++){
									if(tempfile[i][k]==' ')
									{
										store2[m1++][n1]='\0';
										n1=0;
										continue;
									}
									if(tempfile[i][k]!='\n' && tempfile[i][k]!='"')
										store2[m1][n1++]=tempfile[i][k];
								}
								store2[m1][n1]='\0';

								//cout<<"ji"<<endl;
								for(k=0;k<num_columns-1;k++)
								{
									if(whereflag==1){
										if(strcmp(type,"varchar")==0)
										{
											if(strcmp(where[1],"=")==0)
											{
												if(strcmp(store1[wnum-1],store2[wnum1-1])==0)
												{
													if(belongto[k]==0)
													{
													if(writeflag==1){
														write(fd7, store1[colnum[k]-1], strlen(store1[colnum[k]-1])); 
														write(fd7," ",1); 
													}
													else
														printf("%s ",store1[colnum[k]-1]);
													}
												}
											}
										}
										else if(strcmp(type,"int")==0)
										{
											if(strcmp(where[1],"=")==0)
											{
												if(strcmp(store1[wnum-1],store2[wnum1-1])==0)
												{
													if(belongto[k]==0){
													if(writeflag==1){
														write(fd7, store1[colnum[k]-1], strlen(store1[colnum[k]-1])); 
														write(fd7," ",1); 
													}
													else
														printf("%s ",store1[colnum[k]-1]);
													}
												}
											}
										}
									}
									else{
										if(belongto[k]==0){
											if(writeflag==1){
												write(fd7, store1[colnum[k]-1], strlen(store1[colnum[k]-1])); 
												write(fd7," ",1); 
											}
											else
												printf("%s ",store1[colnum[k]-1]);
										}
									}
								}
								if(belongto[k]==0){

									if(disflag==1)
									{
										if(myarr1.count(store1[colnum[k]-1])){}
										else{
											myarr1.insert(store1[colnum[k]-1]);
											if(writeflag==1){
												write(fd7, store1[colnum[k]-1], strlen(store1[colnum[k]-1])); 
												write(fd7," ",1); 
											}
											else
												printf("%s ",store1[colnum[k]-1]);
										}
									}
									else{
										if(whereflag==1){
											if(strcmp(type,"varchar")==0)
											{
												if(strcmp(where[1],"=")==0)
												{
													if(strcmp(store1[wnum-1],store2[wnum1-1])==0)
													{
														if(writeflag==1){
															write(fd7, store1[colnum[k]-1], strlen(store1[colnum[k]-1])); 
															write(fd7," ",1); 
														}
														else
															printf("%s ",store1[colnum[k]-1]);
													}
												}
											}
											else if(strcmp(type,"int")==0)
											{
												if(strcmp(where[1],"=")==0)
												{
													if(strcmp(store1[wnum-1],store2[wnum1-1])==0)
													{
														if(writeflag==1){
															write(fd7, store1[colnum[k]-1], strlen(store1[colnum[k]-1])); 
															write(fd7," ",1); 
														}
														else
															printf("%s ",store1[colnum[k]-1]);
													}
												}
											}
										}
										else{
											if(belongto[k]==0){
												if(writeflag==1){
													write(fd7, store1[colnum[k]-1], strlen(store1[colnum[k]-1])); 
													write(fd7," ",1); 
												}
												else
													printf("%s ",store1[colnum[k]-1]);
											}
										}

									}

								}

								if(whereflag==1){
									if(strcmp(type,"varchar")==0)
									{
										if(strcmp(where[1],"=")==0)
										{
											if(strcmp(store1[wnum-1],store2[wnum1-1])==0)
											{
												if(writeflag==1){
													write(fd7,tempfile[i], strlen(tempfile[i])); 
													write(fd7,"\n",1); 
												}
												else
													printf("%s\n",tempfile[i]);
											}
										}
									}
									else if(strcmp(type,"int")==0)
									{
										if(strcmp(where[1],"=")==0)
										{
											if(strcmp(store1[wnum-1],store2[wnum1-1])==0)
											{
												if(writeflag==1){
													write(fd7,tempfile[i], strlen(tempfile[i])); 
													write(fd7,"\n",1); 
												}
												else
													printf("%s\n",tempfile[i]);
											}
										}
									}
								}
								else{
									if(writeflag==1){
										write(fd7,tempfile[i], strlen(tempfile[i])); 
										write(fd7,"\n",1); 
									}
									else
										printf("%s\n",tempfile[i]);
								}


						}
							strcpy(str1,"");
						}

						else if(c2=='"')continue;
						else
							strncat(str1,&c2,1);
					}
					close(fd4);
				}


				if(chkflag==1 && tabflag!=3 && chktable==1)
				{
					set <string> myarr;
					int fd2,n;	
					char store[10][500];
					fd2 = open(file, O_RDONLY);
					char c1;
					char str[1000];
					while(n = read(fd2, &c1, 1) > 0) {
						if(c1=='\n'){
							int k;
							int m=0,n=0;
							for(k=0;k<strlen(str);k++){
								if(str[k]==',')
								{
									store[m++][n]='\0';
									n=0;
									continue;
								}
								if(str[k]!='\n' && str[k]!='"')
									store[m][n++]=str[k];
							}
							store[m][n]='\0';
							for(k=0;k<num_columns-1;k++)
							{
								if(whereflag==1 && match==0)
								{
									if(strcmp(type,"varchar")==0)
									{
										if(strcmp(where[1],"=")==0)
										{
											if(strcmp(store[wnum-1],where[2])==0){
												if(writeflag==1){
													write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
													write(fd7," ",1); 
												}
												else
													printf("%s ",store[colnum[k]-1]);
											}
										}
									}
									else if(strcmp(type,"int")==0)
									{
										if(strcmp(where[1],"=")==0)
										{
											if(strcmp(store[wnum-1],where[2])==0){
												if(writeflag==1){
													write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
													write(fd7," ",1); 
												}
												else
													printf("%s ",store[colnum[k]-1]);
											}
										}
										else if(strcmp(where[1],">")==0)
										{
											int data=atoi(store[wnum-1]);
											int given=atoi(where[2]);
											if(data>given){
												if(writeflag==1){
													write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
													write(fd7," ",1); 
												}
												else
													printf("%s ",store[colnum[k]-1]);
											}
										}
										else if(strcmp(where[1],">=")==0)
										{
											int data=atoi(store[wnum-1]);
											int given=atoi(where[2]);
											if(data>=given){
												if(writeflag==1){
													write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
													write(fd7," ",1); 
												}
												else
													printf("%s ",store[colnum[k]-1]);
											}
										}
										else if(strcmp(where[1],"<")==0)
										{
											int data=atoi(store[wnum-1]);
											int given=atoi(where[2]);
											if(data<given){
												if(writeflag==1){
													write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
													write(fd7," ",1); 
												}
												else
													printf("%s ",store[colnum[k]-1]);
											}
										}
										else if(strcmp(where[1],"<=")==0)
										{
											int data=atoi(store[wnum-1]);
											int given=atoi(where[2]);
											if(data<=given){
												if(writeflag==1){
													write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
													write(fd7," ",1); 
												}
												else
													printf("%s ",store[colnum[k]-1]);
											}
										}
									}
								}
								else if(whereflag==1 && match==1){
									if(strcmp(type,"varchar")==0)
									{
										if(strcmp(where[1],"=")==0)
										{
											if(strcmp(store[wnum-1],store[wnum1-1])==0)
											{
												if(writeflag==1){
													write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
													write(fd7," ",1); 
												}
												else
													printf("%s ",store[colnum[k]-1]);
											}
										}
									}
									else if(strcmp(type,"int")==0)
									{
										if(strcmp(where[1],"=")==0)
										{
											if(strcmp(store[wnum-1],store[wnum1-1])==0)
											{
												if(writeflag==1){
													write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
													write(fd7," ",1); 
												}
												else
													printf("%s ",store[colnum[k]-1]);
											}
										}
										else if(strcmp(where[1],">")==0)
										{
											int data1=atoi(store[wnum-1]);
											int data2=atoi(store[wnum1-1]);
											if(data1>data2){
												if(writeflag==1){
													write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
													write(fd7," ",1); 
												}
												else
													printf("%s ",store[colnum[k]-1]);
											}
										}
										else if(strcmp(where[1],">=")==0)
										{
											int data1=atoi(store[wnum-1]);
											int data2=atoi(store[wnum1-1]);
											if(data1>=data2)
											{
												if(writeflag==1){
													write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
													write(fd7," ",1); 
												}
												else
													printf("%s ",store[colnum[k]-1]);
											}
										}
										else if(strcmp(where[1],"<")==0)
										{
											int data1=atoi(store[wnum-1]);
											int data2=atoi(store[wnum1-1]);
											if(data1<data2){
												if(writeflag==1){
													write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
													write(fd7," ",1); 
												}
												else
													printf("%s ",store[colnum[k]-1]);
											}
										}
										else if(strcmp(where[1],"<=")==0)
										{
											int data1=atoi(store[wnum-1]);
											int data2=atoi(store[wnum1-1]);
											if(data1<=data2){
												if(writeflag==1){
													write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
													write(fd7," ",1); 
												}
												else
													printf("%s ",store[colnum[k]-1]);
											}
										}

									}
								}
								else{		
									if(writeflag==1){
										write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
										write(fd7," ",1); 
									}
									else
										printf("%s ",store[colnum[k]-1]);
								}
							}
							if(disflag==1)
							{
								if(myarr.count(store[colnum[k]-1])){}
								else{
								if(whereflag==1 && match==0)
								{
									if(strcmp(type,"varchar")==0)
									{
										if(strcmp(where[1],"=")==0)
										{
											if(strcmp(store[wnum-1],where[2])==0){
												myarr.insert(store[colnum[k]-1]);
												if(writeflag==1){
													write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
													write(fd7,"\n",1); 
												}
												else
													printf("%s\n",store[colnum[k]-1]);
											}
										}
									}
									else if(strcmp(type,"int")==0)
									{
										if(strcmp(where[1],"=")==0)
										{
											if(strcmp(store[wnum-1],where[2])==0){
												myarr.insert(store[colnum[k]-1]);
												if(writeflag==1){
													write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
													write(fd7,"\n",1); 
												}
												else
													printf("%s\n",store[colnum[k]-1]);
											}
										}
										else if(strcmp(where[1],">")==0)
										{
											int data=atoi(store[wnum-1]);
											int given=atoi(where[2]);
											if(data>given){
												myarr.insert(store[colnum[k]-1]);
												if(writeflag==1){
													write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
													write(fd7,"\n",1); 
												}
												else
													printf("%s\n",store[colnum[k]-1]);
											}
										}
										else if(strcmp(where[1],">=")==0)
										{
											int data=atoi(store[wnum-1]);
											int given=atoi(where[2]);
											if(data>=given){
												myarr.insert(store[colnum[k]-1]);
												if(writeflag==1){
													write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
													write(fd7,"\n",1); 
												}
												else
													printf("%s\n",store[colnum[k]-1]);
											}
										}
										else if(strcmp(where[1],"<")==0)
										{
											int data=atoi(store[wnum-1]);
											int given=atoi(where[2]);
											if(data<given){
												myarr.insert(store[colnum[k]-1]);
												if(writeflag==1){
													write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
													write(fd7,"\n",1); 
												}
												else
													printf("%s\n",store[colnum[k]-1]);
											}
										}
										else if(strcmp(where[1],"<=")==0)
										{
											int data=atoi(store[wnum-1]);
											int given=atoi(where[2]);
											if(data<=given){
												myarr.insert(store[colnum[k]-1]);
												if(writeflag==1){
													write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
													write(fd7,"\n",1); 
												}
												else
													printf("%s\n",store[colnum[k]-1]);
											}
										}
									}
								}
								else if(whereflag==1 && match==1){
									if(strcmp(type,"varchar")==0)
									{
										if(strcmp(where[1],"=")==0)
										{
											if(strcmp(store[wnum-1],store[wnum1-1])==0){
												myarr.insert(store[colnum[k]-1]);
												if(writeflag==1){
													write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
													write(fd7,"\n",1); 
												}
												else
													printf("%s\n",store[colnum[k]-1]);
											}
										}
									}
									else if(strcmp(type,"int")==0)
									{
										if(strcmp(where[1],"=")==0)
										{
											if(strcmp(store[wnum-1],store[wnum1-1])==0){
												myarr.insert(store[colnum[k]-1]);
												if(writeflag==1){
													write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
													write(fd7,"\n",1); 
												}
												else
													printf("%s\n",store[colnum[k]-1]);
											}
										}
										else if(strcmp(where[1],">")==0)
										{
											int data1=atoi(store[wnum-1]);
											int data2=atoi(store[wnum1-1]);
											if(data1>data2){
												myarr.insert(store[colnum[k]-1]);
												if(writeflag==1){
													write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
													write(fd7,"\n",1); 
												}
												else
													printf("%s\n",store[colnum[k]-1]);
											}
										}
										else if(strcmp(where[1],">=")==0)
										{
											int data1=atoi(store[wnum-1]);
											int data2=atoi(store[wnum1-1]);
											if(data1>=data2){
												myarr.insert(store[colnum[k]-1]);
												if(writeflag==1){
													write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
													write(fd7,"\n",1); 
												}
												else
													printf("%s\n",store[colnum[k]-1]);
											}
										}
										else if(strcmp(where[1],"<")==0)
										{
											int data1=atoi(store[wnum-1]);
											int data2=atoi(store[wnum1-1]);
											if(data1<data2){
												myarr.insert(store[colnum[k]-1]);
												if(writeflag==1){
													write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
													write(fd7,"\n",1); 
												}
												else
													printf("%s\n",store[colnum[k]-1]);
											}
										}
										else if(strcmp(where[1],"<=")==0)
										{
											int data1=atoi(store[wnum-1]);
											int data2=atoi(store[wnum1-1]);
											if(data1<=data2){
												myarr.insert(store[colnum[k]-1]);
												if(writeflag==1){
													write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
													write(fd7,"\n",1); 
												}
												else
													printf("%s\n",store[colnum[k]-1]);
											}
										}

									}
								}
								else{	
									myarr.insert(store[colnum[k]-1]);
									if(writeflag==1){
										write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
										write(fd7,"\n",1); 
									}
									else
										printf("%s\n",store[colnum[k]-1]);
								}
								/*myarr.insert(store[colnum[k]-1]);
								  if(writeflag==1){
								  write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
								  write(fd7,"\n",1); 
								  }
								  else
								  printf("%s\n",store[colnum[k]-1]);
								  */
								}
							}
							else{
								if(whereflag==1 && match==0)
								{
									if(strcmp(type,"varchar")==0)
									{
										if(strcmp(where[1],"=")==0)
										{
											if(strcmp(store[wnum-1],where[2])==0){
												if(writeflag==1){
													write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
													write(fd7,"\n",1); 
												}
												else
													printf("%s\n",store[colnum[k]-1]);
											}
										}
									}
									else if(strcmp(type,"int")==0)
									{
										if(strcmp(where[1],"=")==0)
										{
											if(strcmp(store[wnum-1],where[2])==0){
												if(writeflag==1){
													write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
													write(fd7,"\n",1); 
												}
												else
													printf("%s\n",store[colnum[k]-1]);
											}
										}
										else if(strcmp(where[1],">")==0)
										{
											int data=atoi(store[wnum-1]);
											int given=atoi(where[2]);
											if(data>given){
												if(writeflag==1){
													write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
													write(fd7,"\n",1); 
												}
												else
													printf("%s\n",store[colnum[k]-1]);
											}
										}
										else if(strcmp(where[1],">=")==0)
										{
											int data=atoi(store[wnum-1]);
											int given=atoi(where[2]);
											if(data>=given){
												if(writeflag==1){
													write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
													write(fd7,"\n",1); 
												}
												else
													printf("%s\n",store[colnum[k]-1]);
											}
										}
										else if(strcmp(where[1],"<")==0)
										{
											int data=atoi(store[wnum-1]);
											int given=atoi(where[2]);
											if(data<given){
												if(writeflag==1){
													write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
													write(fd7,"\n",1); 
												}
												else
													printf("%s\n",store[colnum[k]-1]);
											}
										}
										else if(strcmp(where[1],"<=")==0)
										{
											int data=atoi(store[wnum-1]);
											int given=atoi(where[2]);
											if(data<=given){
												if(writeflag==1){
													write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
													write(fd7,"\n",1); 
												}
												else
													printf("%s\n",store[colnum[k]-1]);
											}
										}
									}
								}
								else if(whereflag==1 && match==1){
									if(strcmp(type,"varchar")==0)
									{
										if(strcmp(where[1],"=")==0)
										{
											if(strcmp(store[wnum-1],store[wnum1-1])==0){
												if(writeflag==1){
													write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
													write(fd7,"\n",1); 
												}
												else
													printf("%s\n",store[colnum[k]-1]);
											}
										}
									}
									else if(strcmp(type,"int")==0)
									{
										if(strcmp(where[1],"=")==0)
										{
											if(strcmp(store[wnum-1],store[wnum1-1])==0){
												if(writeflag==1){
													write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
													write(fd7,"\n",1); 
												}
												else
													printf("%s\n",store[colnum[k]-1]);
											}
										}
										else if(strcmp(where[1],">")==0)
										{
											int data1=atoi(store[wnum-1]);
											int data2=atoi(store[wnum1-1]);
											if(data1>data2){
												if(writeflag==1){
													write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
													write(fd7,"\n",1); 
												}
												else
													printf("%s\n",store[colnum[k]-1]);
											}
										}
										else if(strcmp(where[1],">=")==0)
										{
											int data1=atoi(store[wnum-1]);
											int data2=atoi(store[wnum1-1]);
											if(data1>=data2){
												if(writeflag==1){
													write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
													write(fd7,"\n",1); 
												}
												else
													printf("%s\n",store[colnum[k]-1]);
											}
										}
										else if(strcmp(where[1],"<")==0)
										{
											int data1=atoi(store[wnum-1]);
											int data2=atoi(store[wnum1-1]);
											if(data1<data2){
												if(writeflag==1){
													write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
													write(fd7,"\n",1); 
												}
												else
													printf("%s\n",store[colnum[k]-1]);
											}
										}
										else if(strcmp(where[1],"<=")==0)
										{
											int data1=atoi(store[wnum-1]);
											int data2=atoi(store[wnum1-1]);
											if(data1<=data2){
												if(writeflag==1){
													write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
													write(fd7,"\n",1); 
												}
												else
													printf("%s\n",store[colnum[k]-1]);
											}
										}

									}
								}
								else{	
									if(writeflag==1){
										write(fd7, store[colnum[k]-1], strlen(store[colnum[k]-1])); 
										write(fd7,"\n",1); 
									}
									else
										printf("%s\n",store[colnum[k]-1]);
								}
							}
							strcpy(str,"");
						}

						else if(c=='"')continue;
						else
							strncat(str,&c1,1);
					}

					//printf("colflag=%d \n",chkflag);
					close(fd2);
				}
				if(chkflag==0 || chktable==0)
					printf("Invalid query\n");
				gflag=0;
				chkflag=0;
				if(writeflag==1)
				close(fd7);

			}

			whereflag=0;
			writeflag=0;
			num_where=0;
			disflag=0;
			printf("\n<%s> ",sqlprompt);
			bzero(tmp, sizeof(tmp));
		}
		else
			strncat(tmp, &c, 1);


	}
	return 0;

}
