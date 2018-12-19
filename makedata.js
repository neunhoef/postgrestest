db._drop("c");
var c = db._create("c");

var rand = require("internal").rand;
var time = require("internal").time;

var makeRandomString = function() {
  var r = rand();
  var d = rand();
  var s = "x";
  for (var i = 0; i < 100; ++i) {
    s += r;
    r += d;
  }
  return s;
}
 
var writeDocs = function(n) {
  var l = [];
  var times = [];

  for (var i = 0; i < n; ++i) {
    l.push({_key:"K"+i, Hallo:i, s:makeRandomString()});
    if (l.length % 10000 === 0) {
      t = time();
      c.insert(l);
      t2 = time();
      l = [];
      print(i+1, t2-t);
      times.push(t2-t);
    }
  }
  times = times.sort(function(a, b) { return a-b; });
  print(" Median:", times[Math.floor(times.length / 2)], "\n",
        "90%ile:", times[Math.floor(times.length * 0.90)], "\n",
        "99%ile:", times[Math.floor(times.length * 0.99)], "\n",
        "min   :", times[0], "\n",
        "max   :", times[times.length-1]);
}

var writeDocsOverwrite = function(n) {
  var l = [];
  var times = [];

  var j = 0;
  for (var i = 0; i < n; ++i) {
    l.push({_key:"K"+j, Hallo:j, s:makeRandomString()});
    if (l.length % 10000 === 0) {
      t = time();
      c.insert(l, {overwrite:true});
      t2 = time();
      l = [];
      print(i+1, t2-t);
      times.push(t2-t);
    }
    j += 99991;  // a prime!
    while (j > n) {
      j -= n;
    }
  }
  times = times.sort(function(a, b) { return a-b; });
  print(" Median:", times[Math.floor(times.length / 2)], "\n",
        "90%ile:", times[Math.floor(times.length * 0.90)], "\n",
        "99%ile:", times[Math.floor(times.length * 0.99)], "\n",
        "min   :", times[0], "\n",
        "max   :", times[times.length-1]);
}

var randomReads = function(m, n) {
  var firsts = [];
  var caches = [];
  var keys = [];
  for (var i = 0; i < m; ++i) {
    var k = "K" + (Math.abs(rand()) % n);
    keys.push(k);
    var t1 = time();
    var d = c.document(k);
    var t2 = time();
    firsts.push(t2-t1);
    var l = [];
    for (var j = 0; j < 100; j++) {
      t1 = time();
      d = c.document(k);
      t2 = time();
      l.push(t2-t1);
    }
    l = l.sort();
    caches.push([l[0],l[50],l[90],l[99]]);
  }
  firsts = firsts.sort(function(a, b) { return a-b; });
  print("Firsts:\n",
        "Median:", firsts[Math.floor(firsts.length / 2)], "\n",
        "90%ile:", firsts[Math.floor(firsts.length * 0.90)], "\n",
        "99%ile:", firsts[Math.floor(firsts.length * 0.99)], "\n",
        "min   :", firsts[0], "\n",
        "max   :", firsts[firsts.length-1]);
  caches = caches.sort(function(a, b) { return a[1] - b[1]; });
  return {firsts, caches, keys};
}
