//set of variables for basic dataloader ingestion example

.mi.tasks:1!flip`taskID`batchID`worker`taskSize`startTime`endTime`status`task`retries`args`success`result!"jssjppssj*b*"$\:();
.mi.workers:1!flip`worker`id`time`port`handle`pid`task`lastTask`taskID`taskSize`mb`processed!"sjpjiissjjjj"$\:();
.mi.files:1!flip`index`time`dir`file`status`validationKey!"jpsssj"$\:();
.mi.uniqueSymbols:([batchID:0#`]uniqueSymbolsAcrossCols:());
.mi.batchGroupMap:(0#`)!0#`;

 
workers:([]worker:`worker1`worker2`worker3);
.mi.workers:`worker xkey`id`time xcols update id:i+1,time:0Np,port:5100 5101 5102,handle:0Ni,pid:0Ni,task:`,lastTask:`,taskID:0N,taskSize:0N,mb:0N,processed:0 from workers;    
.mi.mem:([]taskID:0#0;worker:0#`;task:0#`;startTime:0Np;endTime:0Np;taskSize:0#0N;mb:0#0);
.mi.largeFileWorker:`;
 
n:count filesList:filesList :(`$":marketData1of3_2019.05.11.csv" ;`$":marketData2of3_2019.05.11.csv";`$":marketData3of3_2019.05.11.csv");


.mi.files:.mi.files upsert ([index:1 2 3]; time:n#.z.p; file:filesList;status:n#`new);
.mi.files:update taskSize:7h$%[;1e6]hcount each file from .mi.files;
matches:select from .mi.files where status=`new;
matches:update readFunction:3#`.mi.read,postReadFunction:3#`.mi.postRead from matches;
fileDate:2019.05.11;
taskSize:7h$%[;1e6]hcount each filesList;
.mi.id:1;
taskIDS:.mi.id+til n;
batchIDs:`$string[n#1?0ng];
.mi.tasks:.mi.tasks upsert flip (`taskID`batchID`worker`taskSize`startTime`endTime`status`task`retries`args`success`result!(taskIDS;batchIDs;n#`;taskSize;n#0Np;n#0Np;n#`queued;n#`.mi.readAndSave;n#0;select dir,file,readFunction,postReadFunction,activeDate:n#fileDate,batchID:batchIDs from matches;n#0b;n#(::)));

h:{hopen `$"::",string[x]} each exec port from `.mi.workers;
update handle:h from `.mi.workers;

mem:100;
.mi.memoryBuffer:50;
.mi.fileSizeLimit:50;
.mi.freeMemoryFree:80;




