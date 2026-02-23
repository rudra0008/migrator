const singleForm = document.getElementById('single-form');
const batchForm = document.getElementById('batch-form');
const singleMessage = document.getElementById('single-message');
const batchMessage = document.getElementById('batch-message');
const jobsTable = document.getElementById('jobs-table');
const logsDialog = document.getElementById('logs-dialog');
const logsContent = document.getElementById('logs-content');
const logsTitle = document.getElementById('logs-title');
const closeLogs = document.getElementById('close-logs');

let currentLogsJobId = null;
let logsPollTimer = null;

function stopLogsPolling() {
  if (logsPollTimer) {
    clearInterval(logsPollTimer);
    logsPollTimer = null;
  }
}

closeLogs.addEventListener('click', () => {
  logsDialog.close();
});

logsDialog.addEventListener('close', () => {
  currentLogsJobId = null;
  stopLogsPolling();
});

singleForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  singleMessage.textContent = 'Submitting single migration...';

  const formData = new FormData(singleForm);
  const response = await fetch('/single', { method: 'POST', body: formData });
  const data = await response.json();

  if (!response.ok) {
    singleMessage.textContent = data.error || 'Failed to submit single migration.';
    singleMessage.className = 'error';
    return;
  }

  singleMessage.textContent = `${data.message}: ${data.job_id}`;
  singleMessage.className = 'success';
  singleForm.reset();
  pollJobs();
});

batchForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  batchMessage.textContent = 'Submitting batch migration...';

  const formData = new FormData(batchForm);
  const response = await fetch('/upload', { method: 'POST', body: formData });
  const data = await response.json();

  if (!response.ok) {
    batchMessage.textContent = data.error || 'Failed to submit batch migration.';
    batchMessage.className = 'error';
    return;
  }

  batchMessage.textContent = `${data.message}: ${data.jobs} jobs queued (batch ${data.batch_id}).`;
  batchMessage.className = 'success';
  batchForm.reset();
  pollJobs();
});

async function cancelJob(jobId) {
  const response = await fetch(`/jobs/${jobId}/cancel`, { method: 'POST' });
  const data = await response.json();

  if (!response.ok) {
    alert(data.message || 'Unable to cancel migration.');
  }
  await pollJobs();
}

async function fetchLogs(jobId) {
  const response = await fetch(`/jobs/${jobId}/logs`);
  const data = await response.json();

  if (!response.ok) {
    logsContent.textContent = data.error || 'Unable to load logs.';
    return;
  }

  const isNearBottom = logsContent.scrollTop + logsContent.clientHeight >= logsContent.scrollHeight - 12;
  logsContent.textContent = data.logs;
  if (isNearBottom) {
    logsContent.scrollTop = logsContent.scrollHeight;
  }
}

async function showLogs(jobId) {
  currentLogsJobId = jobId;
  logsTitle.textContent = `Job: ${jobId}`;
  logsContent.textContent = 'Loading logs...';

  if (!logsDialog.open) {
    logsDialog.showModal();
  }

  stopLogsPolling();
  await fetchLogs(jobId);
  logsPollTimer = setInterval(async () => {
    if (!currentLogsJobId || !logsDialog.open) {
      stopLogsPolling();
      return;
    }
    await fetchLogs(currentLogsJobId);
  }, 2000);
}

function escapeHtml(value) {
  return (value ?? '')
    .toString()
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

function renderMsgsLeft(msgsLeft) {
  return `<span class="msgs-left">${escapeHtml(msgsLeft || "-")}</span>`;
}

async function pollJobs() {
  const response = await fetch('/api/jobs');
  const jobs = await response.json();

  jobsTable.innerHTML = '';

  for (const job of jobs) {
    const row = document.createElement('tr');
    const canCancel = job.status === 'queued' || job.status === 'running';

    row.innerHTML = `
      <td><code>${escapeHtml(job.id)}</code></td>
      <td>${escapeHtml(job.source_user)}</td>
      <td>${escapeHtml(job.source_server)}</td>
      <td>${escapeHtml(job.destination_user)}</td>
      <td>${escapeHtml(job.destination_server)}</td>
      <td><span class="badge badge-${escapeHtml(job.status)}">${escapeHtml(job.status)}</span></td>
      <td>${renderMsgsLeft(job.msgs_left)}</td>
      <td>${escapeHtml(job.error || '')}</td>
      <td class="actions">
        <button class="secondary outline" data-log-job-id="${escapeHtml(job.id)}">Logs</button>
        ${canCancel ? `<button class="secondary outline" data-job-id="${escapeHtml(job.id)}">Cancel</button>` : ''}
      </td>
    `;
    jobsTable.appendChild(row);
  }

  document.querySelectorAll('button[data-job-id]').forEach((button) => {
    button.addEventListener('click', () => cancelJob(button.dataset.jobId));
  });

  document.querySelectorAll('button[data-log-job-id]').forEach((button) => {
    button.addEventListener('click', () => showLogs(button.dataset.logJobId));
  });
}

setInterval(pollJobs, 2500);
pollJobs();
