//set up various env variables and define functions
.mi.hdbTmp:`:tmp;
.mi.hdbDir:`:hdb;
.mi.ihdb:`:merge;
.mi.reloadSym:{x};
.mi.minType:{[typs;sizes;x]typs sizes bin x}[4 5 6 7h;0,7h$2 xexp/:8 16 32-1];
.mi.getIndexDir:{[batchID;typ;dt;t]` sv .mi.hdbTmp,`indx,batchID,typ,(`$string dt),t};
.mi.getIndex:{[batchID;typ;dt;t] $[99=type indx:.mi.indexMap[k:(batchID;typ;dt;t);`index];indx;[.mi.indexMap[k;`index]:indx:get .mi.getIndexDir[batchID;typ;dt;t];indx]]};
.mi.getMergeDB:{[t;typ;dt]` sv .mi.ihdb,typ,(`$string dt),t};
.mi.indexMap:4!enlist`batchID`typ`dt`t`index!(`;`;0Nd;`;::);	

.mi.sendToFreeWorker:{[taskIDs]
    if[count workers:0!select from .mi.workers where null task,not null handle;	
	mem:7h$.mi.fileSizeLimit * .95 1 1.05 sum(and)scan .mi.freeMemoryFree>.mi.memoryBuffer*1 2;
    workerInfo:update worker:` from `taskSize xdesc select taskID,taskSize,task from ([]taskID:taskIDs)#.mi.tasks;
    //assign largest task to dedicated worker
    if[all(count readWrites:exec i from workerInfo where task=`.mi.readAndSave;not null .mi.largeFileWorker);if[null .mi.workers[.mi.largeFileWorker]`task;workerInfo[first readWrites;`worker]:.mi.largeFileWorker;            mem:0|mem-0^workerInfo[0]`taskSize]];		
    /workerInfo:delete task from workerInfo;
	0N!"checking memory";
    mem:0|mem-exec sum 0^taskSize from .mi.tasks where status=`processing;
    if[.mi.largeFileWorker in workerInfo`worker;workers:delete from workers where worker=.mi.largeFileWorker];
    //check number of tasks that can be sent based on memory and then send tasks
    toRest:workerInfo except toLargeWorker:select from workerInfo where not null worker;
    toRest:a neg[n]sublist where mem>(n:count[.mi.workers]-count toLargeWorker)msum(a:reverse toRest)`taskSize;
    toRest:count[workers]sublist toRest;
	0N!".mi.sendToFreeWorker Sending tasks to workers";
    toRest:update worker:count[toRest]# workers`worker from toRest;
	.mi.send each .eg.sendToFree:(toLargeWorker,toRest)lj delete taskID,taskSize from .mi.workers];
   };


.mi.send:{[x]  
    taskInfo:(`taskID xkey .mi.tasks) x`taskID;
   	neg[h:x`handle](`.mi.runTask;(`task`args#taskInfo),(1#`taskID)#x);
    .mi.workers:update task:x`task,taskID:x`taskID from .mi.workers where worker=x`worker;
    .mi.tasks:update taskIDstartTime:.z.p,status:`processing from .mi.tasks where taskID=x`taskID;
   };
   
.eg.workerResponse:()!();   
   
.mi.workerResponse:{[x]
	.eg.workerResponse,:(enlist x`taskID)!enlist x;
	0N!"Running .mi.workerResponse";
   res:x`result;
   status:stat:`failed`complete x`success;  
   if[stat=`failed;0N!"Task Failed";:()];
   if[null(taskInfo:@[.mi.tasks x`taskID;`taskID;:;x`taskID])`batchID;:()];
   .mi.workers:update time:.z.p,task:`,lasttask:task,mb:x`mb,processed+1,taskSize:0N from .mi.workers where worker in (exec worker from .mi.workers where taskID=x`taskID);
 	.mi.tasks:update status:stat,endTime:.z.p,result:enlist x[`result],success:first x[`success] from .mi.tasks where taskID=first  x[`taskID];
   
   if[x[`success]&`.mi.readAndSave=task:taskInfo`task;
		0N!"Read and save complete, caching unique symbols by batch";
       .[`.mi.uniqueSymbols;(taskInfo`batchID;`uniqueSymbolsAcrossCols);{distinct y,x}raze res[`rvalid;`uniqueSymbolsAcrossCols]];
       .mi.analyseReadWrites taskInfo`batchID];
	   0N!"Running Checking if index/merge job pending";
	   
	   if[count  queued:0!select from .mi.tasks where not status=`complete,task in `.mi.index`.mi.merge`.mi.move, i=min i;     
	   0N!"Sending task";
	   .eg.queued:queued;
		.mi.sendToFreeWorker queued`taskID];
	if[not count queued;0N!"Nothing to run, all tasks complete";:()];
   };
   
   
.mi.analyseReadWrites:{[batch]
	$[all `complete=exec status from .mi.tasks where batchID=batch;
	readWrites:0!select from .mi.tasks where task=`.mi.readAndSave,batchID=batch,status=`complete,endTime = (last;endTime) fby args[;`file];
	:()];
	.mi.appendToSymFile batch;
	0N!"sym file appending done";
	result:readWrites[;`result][;`rvalid];
	b:{[batch;x]update batchID:batch from x}batch;
	written:0!select sum colSizes,typ,date,last symCol,last sortCol,allCols:key last colSizes,last symbolCols,paths:path by t from raze result`written;
	0N!"getting indx tasks";
	indxJobSizes:{[a]sum a[`colSizes]c where not null c:a`sortCol`symCol}each written;
	0N!"getting merge tasks";
	toMerge:(ungroup select t,mergeCol:key each colSizes,colSize:get each colSizes from written)lj 1!select t,symCol,sortCol,typ,paths,date,allCols,symbolCols from written;
	toMerge:b select from toMerge where mergeCol<>symCol,mergeCol<>sortCol;
	toIndx:b select from written;
	toMove:b select t,typ,date,flat:0b from written;
	/.mi.copySym[1b;.mi.hdbDir;.mi.hdbTmp];
	.mi.tasks:.mi.tasks upsert update taskID:(1+max (0!.mi.tasks)[`taskID])+ til count i,batchID:batch,taskSize:indxJobSizes,status:`queued,task:`.mi.index,retries:0,result:(::) from flip enlist[`args]!enlist toIndx;
	.mi.tasks:.mi.tasks upsert update taskID:(1+max (0!.mi.tasks)[`taskID]),batchID:batch,taskSize:toMerge[`colSize],status:`queued,task:`.mi.merge,retries:0,result:(::) from flip enlist[`args]!enlist toMerge;
	.mi.tasks:.mi.tasks upsert update taskID:(1+max (0!.mi.tasks)[`taskID]),batchID:batch,taskSize:0,status:`queued,task:`.mi.move,retries:0,result:(::) from flip enlist[`args]!enlist toMove;
	.mi.tasks:update taskSize:7h$%[taskSize;1e6] from .mi.tasks where not task=`.mi.readAndSave;
	0N!"Analyse read writes complete and tasks added to .mi.tasks";	

					};
					
.mi.index:{[x]
     /.mi.reloadSym x`batchID;			
	 load ` sv  .mi.hdbDir,`sym
     {[x;dt;di]   		
         srt:not null sc:first x`sortCol;
         syms:();sorts:();
         if[.mi.fileExists eSymPath:` sv (eroot:.mi.hdbDir,(`$string dt),x`t),x`symCol;
             syms,:get get eSymPath;
             if[srt;sorts,:get` sv eroot,sc]]; 	
         syms,:raze get each` sv'(x[`paths]di),'x`symCol;
         sorts,:$[srt;raze get each` sv'(x[`paths]di),'sc;()];
         I:iasc $[srt;([]syms;sorts);syms];
         .mi.getIndexDir[x`batchID;first x[`typ]di;dt;x`t]set .mi.minType[count I]$I;
         symPath:` sv(mdb:.mi.getMergeDB[x`t;first x[`typ]di;dt]),x`symCol;
         set[symPath;`p#`sym$syms I];
         if[srt;set[` sv mdb,sc;sorts I]];
         set[` sv mdb,`.d;key x`colSizes];
     }[x]'[key g;get g:group x`date];
    };

.mi.appendToSymFile:{[batch]
	.eg.appendToSymFile:batch;
	//checks .mi.uniqueSymbols table for batch and appends to sym file
	if[0<count first us:.mi.uniqueSymbols batch;0N!"Appending unique syms to the sym file ",string symFile:` sv  .mi.hdbDir,`sym;
		symFile?us`uniqueSymbolsAcrossCols;
		0N!"Complete. ",string[count us`uniqueSymbolsAcrossCols]," syms appended";
		delete from`.mi.uniqueSymbols where batchID=batch;
		0N!"Finished .mi.appendToSymFile"];
 };					
				
	.mi.move:{[x]
	
	.eg.move:.z.p;
	raze {[x;dt;di]
	sdt:`$string first[dt];
	from:1_string ` sv (.mi.ihdb;first x`typ;sdt;x`t);
	to:1_string .Q.par[.mi.hdbDir;sdt;x`t];;
	
	if[not sdt in key .mi.hdbDir;system "cd ",1_string .mi.hdbDir;system "mkdir ",string sdt;system "cd .."];
	0N!"Moving merged data to HDB";
	system "MOVE ",from," ",to;	
		
	 }[x]'[key g;g:group x`date]				

   };
