import time
import threading
import multiprocessing
import queue
import gc

def BubbleSort( array, tempQueue ) :  # method 1~4
  length = len( array )
  swapped = False

  for i in range( length ) :
    swapped = False

    for j in range( length - i - 1 ) :
      if array[j] > array[j+1] :
        array[j], array[j+1] = array[j+1], array[j]

        swapped = True

    if ( swapped == False ) :
      break

  tempQueue.put( array )  # method 1 doesn't needed

def CutArray( array, cut ) :  # method 2~4
  cutArrays = []
  partition = int( len( array ) / cut )  # item / piece
  remainder = int( len( array ) % cut )

  index = 0
  while len( cutArrays ) < cut :
    cutArray = []

    i = 0
    while i < partition :
      cutArray.append( array[ index + i ] )
      i = i + 1

    if ( remainder > 0 ) :
      cutArray.append( array[ index + i ] )
      index = index + 1
      remainder = remainder - 1

    cutArrays.append( cutArray )
    index = index + partition

    del cutArray
    gc.collect()

  return cutArrays

def MergeSortForThreadAndProcess( leftArray, rightArray, tempQueue ): # method 2, 3
  mergeArray = []  # sorted list
  l, r = 0, 0  # index of left, right array
  lenOf_leftArray = len( leftArray )
  lenOf_rightArray = len( rightArray )

  while l < lenOf_leftArray and r < lenOf_rightArray : # breaks if one of the list is empty !
    if leftArray[l] <= rightArray[r] :  # stable
      mergeArray.append( leftArray[l] )
      l += 1
    else :
      mergeArray.append( rightArray[r] )
      r += 1

  if l == lenOf_leftArray :  # leftArray is empty
      mergeArray.extend( rightArray[ r : lenOf_rightArray ] )
  else :
      mergeArray.extend( leftArray[ l : lenOf_leftArray ] )

  tempQueue.put( mergeArray )  # put the sorted list into the queue

  del mergeArray
  gc.collect()

def MergeSortForSingleProcess( cutArrays ) :  # method 4
  if len( cutArrays ) > 1 :  # After pop, cutArrays will be empty
    mergeArray = []
    leftArray = cutArrays.pop(0)
    rightArray = cutArrays.pop(0)

    while len( leftArray ) > 0 or len( rightArray ) > 0 :
      if len( leftArray ) == 0 :
        mergeArray.append( rightArray.pop(0) )

      elif len( rightArray ) == 0 :
        mergeArray.append( leftArray.pop(0) )

      # if L[i]<=R[j]
      elif leftArray[0] > rightArray[0] :
        mergeArray.append( rightArray.pop(0) )

      elif leftArray[0] < rightArray[0]:
        mergeArray.append( leftArray.pop(0) )

      elif leftArray[0] == rightArray[0] :
        mergeArray.append( leftArray.pop(0) )

    cutArrays.append( mergeArray )

    del mergeArray
    gc.collect()

def OutputFile( array, fileName, choice, CPUtime ) : # output to file
  output = open( fileName + "_output" + str(choice) + ".txt", 'w' )
  output.write( "Sort :\n")

  i = 0
  while i < len(array) :
    output.write( str( array[i] ) + "\n" )
    i = i + 1

  output.write( "\nCPU time : " + str(CPUtime) + " sec\n" )
  output.write( "\nOutput Time : " +  str( time.ctime() ) + " +08:00\n" )
  output.close()

def main() :
  while 1 :
    array = []

    fileName = input( "Please enter filename (e.g., input_1w) : " )

    try :
      inputFile = open( fileName + ".txt", 'r' )

    except IOError :
      print( "\n" + fileName + ".txt does not excist !!!\n" )
      continue

    for n in inputFile.read().split() :  # read input file
      array.append( int(n) )

    inputFile.close()

    choice = int( input( "Which method do you like (1,2,3,4) : " ) )

    if choice == 1 :
      tempQueue = queue.Queue()
      startTime = time.time()
      print( "\nRunning function 1...\n" )

      BubbleSort( array, tempQueue )

      endTime = time.time()
      OutputFile( array, fileName, choice, endTime - startTime )

    elif choice == 2 :
      threads = []
      m_threads = []


      cut = int( input( "How many patitions would you like to cut : " ) )
      startTime = time.time()
      print( "\nRunning function 2...\n" )

      cutArrays = CutArray( array, cut )

      q = queue.Queue( cut )

      for i in range( cut ) :
        t = threading.Thread( target = BubbleSort, args = ( cutArrays[i], q ) )
        threads.append(t)

      nBubble, nMerge = 0, 0
      while nBubble < cut or nMerge < cut - 1 : # it have to run k thread of bubble sort and k-1 thread of mergesort
        if nBubble < cut :  # start bubblesort
          threads[nBubble].start()
          nBubble += 1

        if q.qsize() >= 2 : # start mergesort only if the size of q is larger than 1
          ll = q.get()
          rl = q.get()
          mt = threading.Thread( target = MergeSortForThreadAndProcess, args = ( ll, rl, q ) )
          mt.start()
          m_threads.append( mt )
          nMerge += 1

      for m in m_threads : # wait for all thread are terminated
        m.join()

      endTime = time.time()
      OutputFile( q.get(), fileName, choice, endTime - startTime )

      del cutArrays, threads, m_threads
      gc.collect()

    elif choice == 3 :
      processes = []
      m_processes = []

      cut = int( input( "How many patitions would you like to cut : " ) )
      startTime = time.time()
      print( "\nRunning function 3...\n" )

      cutArrays = CutArray( array, cut )

      q = multiprocessing.Manager().Queue( cut )

      for i in range( cut ) :
        p = multiprocessing.Process( target = BubbleSort, args = ( cutArrays[i], q ) )
        processes.append(p)

      nBubble, nMerge = 0,0
      while nBubble < cut or nMerge < cut - 1 : # it have to run k process of bubble sort and k-1 process of mergesort
        if nBubble < cut : # start bubblesort
          processes[nBubble].start()
          nBubble += 1

        if q.qsize() >= 2 :  # start mergesort only if the size of q is larger than 1
          ll = q.get()
          rl = q.get()
          mp = multiprocessing.Process (target = MergeSortForThreadAndProcess, args = ( ll, rl, q ) )
          mp.start()
          m_processes.append( mp )
          nMerge += 1

      for mp in m_processes :  # wait for all process are terminated
        mp.join()

      endTime = time.time()
      OutputFile( q.get(), fileName, choice, endTime - startTime )

    elif choice == 4 :
      tempQueue = queue.Queue()

      cut = int( input( "How many patitions would you like to cut : " ) )
      startTime = time.time()
      print( "\nRunning function 4...\n")

      cutArrays = CutArray( array, cut )

      for array in cutArrays :  # BubbleSort
        BubbleSort( array, tempQueue )

      length = len( cutArrays )
      for _ in range( length - 1 ) :  # MergeSort
        MergeSortForSingleProcess( cutArrays )

      endTime = time.time()
      OutputFile( cutArrays[0], fileName, choice, endTime - startTime )

      del cutArrays
      gc.collect()

    else :
      print( "\nPlease enter 0~4 !!!\n")
      del array
      gc.collect()
      continue

    del array
    gc.collect()

if __name__ == "__main__" :
  main()