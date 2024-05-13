module kvraft

go 1.22.1

require "labgob" v0.0.0
require "labrpc" v0.0.0
require "raft" v0.0.0
require "models" v0.0.0
require "porcupine" v0.0.0

replace "labgob" => "../labgob"
replace "labrpc" => "../labrpc"
replace "raft" => "../raft"
replace "models" => "../models"
replace "porcupine" => "../porcupine"