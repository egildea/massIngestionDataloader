
.mi.hdbTmp:`:tmp;
.mi.hdbDir:`:hdb;
.mi.ihdb:`:merge;
.mi.reloadSym:{x};
.mi.minType:{[typs;sizes;x]typs sizes bin x}[4 5 6 7h;0,7h$2 xexp/:8 16 32-1];
.mi.getIndexDir:{[batchID;typ;dt;t]` sv .mi.hdbTmp,`indx,batchID,typ,(`$string dt),t};
.mi.getIndex:{[batchID;typ;dt;t] $[99=type indx:.mi.indexMap[k:(batchID;typ;dt;t);`index];indx;[.mi.indexMap[k;`index]:indx:get .mi.getIndexDir[batchID;typ;dt;t];indx]]};
.mi.getMergeDB:{[t;typ;dt]` sv .mi.ihdb,typ,(`$string dt),t};
.mi.indexMap:4!enlist`batchID`typ`dt`t`index!(`;`;0Nd;`;::);
.mi.fileExists:not()~key@;
.mi.dirExists:{11h=type key x};

.mi.runTask:{[taskDic]
	.eg.RunTask:taskDic;
    neg[.z.w](`.mi.workerResponse;(`taskID`mb!(taskDic`taskID;7h$%[.Q.w[]`heap;1e6])),
      `success`result!@[{(1b;x[`task]@x`args)};taskDic;{(0b;x)}]);
    neg[.z.w](::);
   };

.mi.readAndSave:{[x]
    .eg.readAndSave:(x;.z.p);
	file:x`file;
    data:x[`readFunction]@file;
    data:x[`postReadFunction]@data;
    .mi.writeTablesByFile[x;data]
  };

.mi.writeTablesByFile:{[x;data]
    if[0=count data;:(enlist `rvalid)!()];
    rvalid:.mi.writeFile[x;data];
    (enlist `rvalid)!(enlist rvalid)
   };

.mi.writeFile:{[x;data]
	.eg.writeFileRun:1b;
	.eg.writeFile:(x;data);
    file:`$last "/" vs  1_string x`file;
    batchID:`$string x`batchID;
    db:` sv .mi.hdbTmp,batchID,file;
    t:data 0;
    c:key f:flip tab:data 1;
    symbolCols:where 11h=type each f;
    nonSymCols:(c:key f)except symbolCols,`date;
    colSizes:.mi.noEnumSplay[apath:` sv db,t;c;nonSymCols;symbolCols;tab];
    written:flip `typ`date`path`colSizes!(enlist `active;x`activeDate;enlist apath;enlist colSizes);
	written:update t:data 0,symCol:`sym,sortCol:`time,symbolCols:count[written]#enlist[symbolCols]from written;
    res:`t`written`uniqueSymbolsAcrossCols!(t;written;distinct raze symbolCols#f)
   };

.mi.noEnumSplay:{[path;allCols;nonSymCols;symCols;x]
	.eg.noEnumSplay:(path;allCols;nonSymCols;symCols;x);
    colSizes:(0#`)!0#0;
    if[count nonSymCols;
        set'[` sv'path,'nonSymCols;flip nonSymCols#x];
        colSizes,:nonSymCols!hcount each` sv'path,'nonSymCols];
    if[count symCols;
        colSizes,:hcount each set'[` sv'path,'key f;f:flip symCols#x]];
    set[` sv path,`.d;allCols];
    allCols#colSizes
   };


.mi.read:{("P SF";enlist ",")0:x};

.mi.postRead:{(`marketTrades;x)};

.mi.index:{[x]
     load ` sv  .mi.hdbDir,`sym;
     {[x;dt;di]
		.eg.index:(x;dt;di);
         srt:not null sc:first x`sortCol;
         syms:();sorts:();
         if[not()~key@ eSymPath:` sv (eroot:.mi.hdbDir,(`$string dt),x`t),x`symCol;             syms,:get get eSymPath;             if[srt;sorts,:get` sv eroot,sc]];
         syms,:raze get each` sv'(x[`paths]di),'x`symCol;
         sorts,:$[srt;raze get each` sv'(x[`paths]di),'sc;()];
         I:iasc $[srt;([]syms;sorts);syms];
         .mi.getIndexDir[x`batchID;first x[`typ]di;dt;x`t]set .mi.minType[count I]$I;
         symPath:` sv(mdb:.mi.getMergeDB[x`t;first x[`typ]di;dt]),x`symCol;
         set[symPath;`p#`sym$syms I];
         if[srt;set[` sv mdb,sc;sorts I]];
         set[` sv mdb,`.d;cols x`colSizes];
     }[x]'[key g;get g:group x`date];
    };


.mi.merge:{[x]
	.eg.x:x;
    load ` sv  .mi.hdbDir,`sym;
    {[x;dt;di]
        data:();
        toPath:` sv .mi.getMergeDB[x`t;first x[`typ]di;dt],mc:x`mergeCol;
		.eg.toPath:toPath;
        if[not()~key@ .eg.epath:epath:` sv .mi.hdbDir,(`$string dt),x[`t],mc;data,:get epath];
        colData:raze get each ` sv'(x[`paths]di),'x`mergeCol;
        data,:$[mc in x`symbolCols;`sym$colData;colData];
		.eg.data:data;
        /data@:.mi.getIndex[x`batchID;first x[`typ]di;dt;x`t];
		dir:` sv  .mi.hdbTmp,`indx,x`batchID;
		data@:get ` sv (dir;`active;`$string first dt;first x`t);
        set[toPath;data];
    }[x]'[key g;get g:group x`date]
   };

.mi.move:{[x]
  .eg.movex:x;
  .eg.move:.z.p;
	g:group x`date;
	raze {[x;dt]
		sdt:`$string first[dt];
		frm:1_string ` sv (.mi.ihdb;first x`typ;sdt;x`t);
		to:1_string .Q.par[.mi.hdbDir;sdt;x`t];
		0N!"Moving merged data to HDB";

		$["w"=first string .z.o;
			[if[not sdt in key .mi.hdbDir;system "cd ",1_string .mi.hdbDir;system "mkdir ",string sdt;system "cd .."];
				mvCmd:"MOVE ",frm," ",to;
				//tidy up directory on windows if table already exists
				if[x[`t] in key tabDir:.Q.par[.mi.hdbDir;sdt;`];
					system "cd ",1_string tabDir;system "rmdir /Q /S ",string[x`t];system "cd ../.."]];
			[system"mkdir -p ",to;
				mvCmd:"mv ",frm,"/{.[!.],}* ",to,"/"]];

		.eg.from:frm;.eg.to:to;
		system mvCmd;
		}[x]'[key g]
 };
