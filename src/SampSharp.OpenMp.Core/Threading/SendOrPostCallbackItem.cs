using Microsoft.Extensions.ObjectPool;

namespace SampSharp.OpenMp.Core;

internal sealed class SendOrPostCallbackItem : IDisposable
{
    private readonly ManualResetEvent _asyncWaitHandle = new(false);
    private ExecutionType _executionType;
    private SendOrPostCallback? _method;
    private ObjectPool<SendOrPostCallbackItem>? _pool;
    private object? _state;

    public Exception? Exception { get; private set; }

    public WaitHandle ExecutionCompleteWaitHandle => _asyncWaitHandle;

    public void Dispose()
    {
        _asyncWaitHandle?.Dispose();
        GC.SuppressFinalize(this);
    }

    public void Set(ExecutionType executionType, SendOrPostCallback method, object? state, ObjectPool<SendOrPostCallbackItem>? returnToPool)
    {
        _executionType = executionType;
        _method = method;
        _state = state;
        _pool = returnToPool;
    }

    public void Reset()
    {
        _asyncWaitHandle.Reset();
        Exception = null;
        _method = null;
        _state = null;
        _pool = null;
    }

    public void Execute()
    {
        try
        {
            var method = _method;

            // Diagnostic guard: a pooled item must never reach Execute() without a
            // delegate. If it does, the item was Reset() between Set() and Execute()
            // — a SynchronizationContext pool/lifecycle race. Previously this threw a
            // bare NullReferenceException whose only stack frame was this method,
            // which is impossible to attribute. Surface it explicitly, and don't kill
            // the tick drain or leave a blocked Send() caller hanging forever.
            if (method == null)
            {
                SampSharpExceptionHandler.HandleException("async",
                    new InvalidOperationException(
                        $"SendOrPostCallbackItem.Execute: callback was null (executionType={_executionType}). " +
                        "Pooled item was reset before execution — SynchronizationContext pool race."));
                _asyncWaitHandle.Set(); // unblock any waiting Send() caller; no-op for Post
                return;
            }

            if (_executionType == ExecutionType.Send)
            {
                try
                {
                    method(_state);
                }
                catch (Exception e)
                {
                    Exception = e;
                }
                finally
                {
                    _asyncWaitHandle.Set();
                }
            }
            else
            {
                // Fire-and-forget: nobody observes Exception, so a throwing handler
                // would otherwise escape to the tick-drain catch as an "unhandled"
                // exception. Capture it here with full detail instead.
                try
                {
                    method(_state);
                }
                catch (Exception e)
                {
                    SampSharpExceptionHandler.HandleException("async-post", e);
                }
            }
        }
        finally
        {
            if (_pool != null)
                _pool.Return(this);
            else if (_executionType == ExecutionType.Post)
                Dispose(); // fire-and-forget item; Send items are disposed by their caller's using
        }
    }
}