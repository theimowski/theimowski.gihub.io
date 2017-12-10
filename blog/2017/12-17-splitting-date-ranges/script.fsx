open System

module ThirdPartyApi =

  type DocType =
  | DocTypeA
  | DocTypeB
  | DocTypeC

  type Document =
    { DocId   : int
      Type    : DocType
      Created : DateTimeOffset
      Content : string
      (* more properties ... *) }

  // 2000-01-01 00:00:00 UTC
  let baseDate = DateTimeOffset(2000, 1, 1, 0, 0, 0, 0, TimeSpan.Zero)

  let documents : Document [] =
    Array.init 100000 (fun i ->
      let typ =
        match i % 3 with
        | 0 -> DocTypeA
        | 1 -> DocTypeB
        | 2 -> DocTypeC
        | _ -> failwith "should not happen"

      { DocId   = i + 1
        Type    = typ
        Created = baseDate + TimeSpan.FromHours (1. * float (i + 1))
        Content = sprintf "Document no. %d" (i + 1) })

  type Filters =
    { Type                 : DocType option
      CreatedBeforeOrEqual : DateTimeOffset option
      CreatedAfter         : DateTimeOffset option
      (* more filters ... *) }

  let globalMaxLimit = 1000

  let search filters =
    async {
      let results =
        documents
        |> Array.filter
          (fun doc -> 
            match filters.Type with
            | Some t -> doc.Type = t 
            | None   -> true)
        |> Array.filter
          (fun doc ->
            match filters.CreatedAfter with
            | Some a -> doc.Created > a
            | None   -> true)
        |> Array.filter
          (fun doc ->
            match filters.CreatedBeforeOrEqual with
            | Some b -> doc.Created <= b
            | None   -> true)

      if results.Length > globalMaxLimit then
        return Array.take globalMaxLimit results
      else
        return results
    }

type DateRange =
| Unbounded
| BeforeOrEqual of DateTimeOffset
| After         of DateTimeOffset
| Between       of DateTimeOffset * DateTimeOffset

type NoLimitSearchError =
| MinDateAfterNow     of minDate : DateTimeOffset * now : DateTimeOffset
| MinTimeSpanExceeded of TimeSpan

let minTimeSpan = TimeSpan.FromSeconds 1.0

let midBetween (dateA : DateTimeOffset) (dateB : DateTimeOffset) =
  let diff = dateB - dateA
  let halfDiff = TimeSpan(diff.Ticks / 2L)
  if halfDiff < minTimeSpan then
    Error (MinTimeSpanExceeded minTimeSpan)
  else
    Ok (dateA + halfDiff)

let getNowDate () =
  DateTimeOffset.UtcNow

let split minDate range =
  let now = getNowDate()
  if minDate > now then
    Error (MinDateAfterNow (minDate, now))
  else
    let dateA, dateB =
      match range with
      | Unbounded       -> minDate, now
      | BeforeOrEqual b -> minDate, b
      | After a         -> a, now
      | Between (b, a)  -> b, a

    let dates mid =
      match range with
      | Unbounded       -> BeforeOrEqual mid, After mid
      | BeforeOrEqual b -> BeforeOrEqual mid, Between (mid, b)
      | After a         -> Between (a, mid), After mid
      | Between (a, b)  -> Between (a, mid), Between (mid, b)

    midBetween dateA dateB
    |> Result.map dates

let fromFilters (filters : ThirdPartyApi.Filters) =
  let before = filters.CreatedBeforeOrEqual
  let after  = filters.CreatedAfter

  match before, after with
  | Some before, Some after -> Between (before, after)
  | Some before, None       -> BeforeOrEqual before
  | None, Some after        -> After after
  | None, None              -> Unbounded

let apply range (filters : ThirdPartyApi.Filters) =
  let filters =
    { filters with 
        CreatedBeforeOrEqual = None
        CreatedAfter         = None }

  match range with
  | Unbounded ->
    filters
  | BeforeOrEqual b ->
    { filters with CreatedBeforeOrEqual = Some b }
  | After a ->
    { filters with CreatedAfter = Some a }
  | Between (a, b) ->
    { filters with
        CreatedAfter         = Some a
        CreatedBeforeOrEqual = Some b }

let searchNoLimit minDate filters =
  let rec search range =
    async {
      let filters' = apply range filters
      let! results = ThirdPartyApi.search filters'
      if results.Length < ThirdPartyApi.globalMaxLimit then
        return Ok results
      else
        match split minDate range with
        | Ok (firstRange, secondRange) ->
          let! results = Async.Parallel [| search firstRange; search secondRange |]
          match results with
          | [| Ok a; Ok b |] -> return Ok (Array.append a b)
          | [| Error e; _ |]
          | [| _; Error e |] -> return Error e
          | _                -> return failwith "unexpected, array should have 2 elems"
        | Error e ->
          return Error e
    }

  search (fromFilters filters)

let onlyDocA : ThirdPartyApi.Filters =
  { Type                 = Some ThirdPartyApi.DocTypeA
    CreatedBeforeOrEqual = None
    CreatedAfter         = None  }

ThirdPartyApi.search onlyDocA
|> Async.RunSynchronously
|> fun results -> printfn "Third Party Api results length: %d" results.Length

let minDate = ThirdPartyApi.baseDate

match searchNoLimit minDate  onlyDocA |> Async.RunSynchronously with
| Ok results ->
  printfn "No Limit results length: %d" results.Length
| Error e ->
  printfn "No Limit failed with: %A" e