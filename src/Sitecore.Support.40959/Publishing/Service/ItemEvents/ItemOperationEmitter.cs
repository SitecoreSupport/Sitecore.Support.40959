namespace Sitecore.Support.Publishing.Service.ItemEvents
{
  using System;
  using System.Collections.Generic;
  using System.Globalization;
  using System.Linq;
  using System.Reactive.Concurrency;
  using System.Reactive.Linq;
  using System.Reactive.Subjects;
  using System.Threading.Tasks;
  using Sitecore.Framework.Publishing.Locators;
  using Sitecore.Framework.Publishing.PublisherOperations;
  using Sitecore.Publishing.Service.ItemEvents;

  public class ItemOperationEmitter : IItemOperationEmitter, IDisposable
  {
    private readonly IPublisherOperationService _publisherOpsService;

    private readonly Subject<ItemOperationData> _itemOperationStream = new Subject<ItemOperationData>();

    private readonly IScheduler _scheduler;
    private readonly int _eventBufferingMaxCount;
    private readonly int _eventBufferingWindowMaxMilliseconds;

    private readonly TaskCompletionSource<bool> _eventStreamCompletion = new TaskCompletionSource<bool>();
    private bool _disposedValue;

    public ItemOperationEmitter(
        IPublisherOperationService publisherOpsService,
        string eventBufferingWindowMaxMilliseconds,
        string eventBufferingMaxCount)
    {
      _scheduler = Scheduler.Default;
      _publisherOpsService = publisherOpsService;
      _eventBufferingWindowMaxMilliseconds = int.Parse(eventBufferingWindowMaxMilliseconds, CultureInfo.InvariantCulture);
      _eventBufferingMaxCount = int.Parse(eventBufferingMaxCount, CultureInfo.InvariantCulture);

      Initialize();
    }

    public void PostOperation(ItemOperationData operationData)
    {
      _itemOperationStream.OnNext(operationData);
    }

    public Task CloseAndFlushAsync()
    {
      _itemOperationStream.OnCompleted();
      return _eventStreamCompletion.Task;
    }

    public void Dispose()
    {
      Dispose(true);
      GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
      if (!_disposedValue)
      {
        if (disposing)
        {
        }

        CloseAndFlushAsync().Wait();
        _itemOperationStream.Dispose();
        _disposedValue = true;
      }
    }

    private void Initialize()
    {
      _itemOperationStream
          .ObserveOn(_scheduler)
          .Buffer(TimeSpan.FromMilliseconds(_eventBufferingWindowMaxMilliseconds), _eventBufferingMaxCount)
          .Where(operations => operations.Any())
          .Subscribe(opsData =>
          {
                  //1) Dedupe operations
                  var deduped = opsData
                      .Reverse()
                      .Distinct(new ItemOperationUriDeduperComparer())
                      .Reverse()
                      .ToArray();

            var dedupedOps = deduped.Select(data => new PublisherOperation(
                      0,
                      data.VariantUri,
                      data.Revision,
                      new ItemPath(data.Path),
                      data.Operation,
                      data.Timestamp,
                      data.Timestamp));

                  //2) Expand to considers after de-duping
                  var expandedOps = deduped
                                      .Where(data => data.Operation != PublisherOperationType.AllVariantsDeleted)
                                      .SelectMany(data => ExpandToConsiderOperations(data));

                  //3) Persist the operations
                  _publisherOpsService.AddPublisherOperations(dedupedOps.Concat(expandedOps).ToArray());
          },
              exception => { _eventStreamCompletion.SetException(exception); },
              () => { _eventStreamCompletion.SetResult(true); });
    }

    private IEnumerable<PublisherOperation> ExpandToConsiderOperations(ItemOperationData data)
    {
      var itemPath = new ItemPath(data.Path);

      if (data.Restrictions.PublishDate > DateTime.UtcNow)
      {
        yield return new PublisherOperation(
            0,
            data.VariantUri,
            data.Revision,
            itemPath,
            PublisherOperationType.ConsiderItem,
            data.Timestamp,
            data.Restrictions.PublishDate);
      }

      if (data.Restrictions.UnpublishDate != DateTime.MaxValue.ToUniversalTime() && data.Restrictions.UnpublishDate > DateTime.UtcNow)
      {
        yield return new PublisherOperation(
            0,
            data.VariantUri,
            data.Revision,
            itemPath,
            PublisherOperationType.ConsiderItem,
            data.Timestamp,
            data.Restrictions.UnpublishDate);
      }

      if (data.Restrictions.ValidFrom > DateTime.UtcNow)
      {
        yield return new PublisherOperation(
            0,
            data.VariantUri,
            data.Revision,
            itemPath,
            PublisherOperationType.ConsiderItem,
            data.Timestamp,
            data.Restrictions.ValidFrom);
      }

      if (data.Restrictions.ValidTo != DateTime.MaxValue.ToUniversalTime() && data.Restrictions.ValidTo > DateTime.UtcNow)
      {
        yield return new PublisherOperation(
            0,
            data.VariantUri,
            data.Revision,
            itemPath,
            PublisherOperationType.ConsiderItem,
            data.Timestamp,
            data.Restrictions.ValidTo);
      }
    }

    private class ItemOperationUriDeduperComparer : IEqualityComparer<ItemOperationData>
    {
      private readonly IItemVariantLocatorComparer _uriComparer;

      public ItemOperationUriDeduperComparer()
      {
        _uriComparer = new IItemVariantLocatorComparer();
      }

      public bool Equals(ItemOperationData x, ItemOperationData y)
      {
        if (x == null || y == null)
        {
          return false;
        }

        return _uriComparer.Equals(x.VariantUri, y.VariantUri);
      }

      public int GetHashCode(ItemOperationData obj)
      {
        return _uriComparer.GetHashCode(obj.VariantUri);
      }
    }
  }
}