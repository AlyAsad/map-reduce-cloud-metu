from celery import group, chord
from tasks import map_chunk, reduce_all, read_chunks

job = chord(group(map_chunk.s(chunk) for chunk in read_chunks()))(reduce_all.s())
print("Submitted!  Jobid:", job)
